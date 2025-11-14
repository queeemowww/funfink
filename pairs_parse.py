import asyncio
import json
import os
from datetime import datetime
from typing import Any, Dict, List

import httpx
import pandas as pd


class Parsing():
    def __init__(self):

        self.TIMEOUT = httpx.Timeout(12.0, connect=12.0, read=12.0)
        self.HEADERS = {
            "User-Agent": "funding-pairs-collector/1.1",
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-US,en;q=0.9,ru;q=0.8",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
            "Connection": "keep-alive",
        }
        self.CONCURRENCY = 8
        # порог суточного объёма в USDT
        self.MIN_24H_USDT = 5_000_000

    # ---------- Утилиты ----------

    async def safe_get(self, d: dict, *path, default=None):
        cur = d
        for k in path:
            if isinstance(cur, dict) and k in cur:
                cur = cur[k]
            else:
                return default
        return cur

    def parse_base_quote_from_symbol(self, sym: str):
        known_quotes = (
            "USDT", "USDC", "USD", "USDⓈ", "BUSD", "BTC", "ETH",
            "EUR", "TRY", "BRL", "TUSD", "FDUSD", "DAI"
        )
        for q in sorted(known_quotes, key=len, reverse=True):
            if sym.endswith(q):
                return sym[: -len(q)].strip("-_/:"), q
        if sym.endswith("-SWAP") and "-" in sym:
            parts = sym.split("-")
            if len(parts) >= 3:
                return parts[0], parts[1]
        return None, None

    def norm_status(self, s) -> str:
        s_low = str(s).lower()
        if any(k in s_low for k in ("trading", "listed", "online", "open", "enabled", "active", "1")):
            return "trading"
        if any(k in s_low for k in ("suspend", "offline", "halt", "delist", "closed", "0", "2")):
            return "suspended"
        return "trading"

    def out_path(self, ext: str) -> str:
        os.makedirs(r"C:\Users\User\my\py\funding_pars\data", exist_ok=True)
        ts = datetime.utcnow().strftime("%Y%m%d_%H%M")
        return os.path.join("data", f"pairs_{ts}.{ext}")

    async def fetch_json(
        self,
        client: httpx.AsyncClient,
        url: str,
        params: dict | None = None,
        retries: int = 3
    ) -> dict:
        last_exc = None
        for i in range(retries):
            try:
                r = await client.get(url, params=params, headers=self.HEADERS)
                r.raise_for_status()
                try:
                    return r.json()
                except json.JSONDecodeError:
                    return {"raw_text": r.text}
            except Exception as e:
                last_exc = e
                await asyncio.sleep(0.8 * (i + 1))
        raise last_exc

    # ---------- Bybit ----------

    async def bybit_pairs(self, client: httpx.AsyncClient) -> List[Dict[str, Any]]:
        """
        Bybit v5: linear (USDT/USDC) + inverse (coin).
        Фильтр по turnover24h (для USDT/USDC контрактов это как раз долларовый объём).
        """
        out: List[Dict[str, Any]] = []
        min_vol = self.MIN_24H_USDT

        async def get_tickers(category: str) -> Dict[str, float]:
            url = "https://api.bybit.com/v5/market/tickers"
            params = {"category": category, "limit": 1000}
            data = await self.fetch_json(client, url, params)
            lst = await self.safe_get(data, "result", "list", default=[]) or []
            vol_map: Dict[str, float] = {}
            for t in lst:
                sym = t.get("symbol")
                if not sym:
                    continue
                v_raw = t.get("turnover24h")
                try:
                    vol = float(v_raw) if v_raw is not None else 0.0
                except (TypeError, ValueError):
                    vol = 0.0
                vol_map[sym] = vol
            return vol_map

        for cat, linear_inverse, margin_asset in [
            ("linear", "linear", None),   # USDT/USDC
            ("inverse", "inverse", None), # BTC/ETH и др.
        ]:
            ticker_vols = await get_tickers(cat)

            url = "https://api.bybit.com/v5/market/instruments-info"
            params = {"category": cat, "limit": 1000}
            data = await self.fetch_json(client, url, params)
            lst = await self.safe_get(data, "result", "list", default=[]) or []
            for it in lst:
                if it.get("contractType") not in ("LinearPerpetual", "InversePerpetual", "Perpetual"):
                    continue
                sym = it.get("symbol")
                if not sym:
                    continue

                vol_usdt = ticker_vols.get(sym, 0.0)
                if vol_usdt < min_vol:
                    continue

                base = it.get("baseCoin")
                quote = it.get("quoteCoin")
                margin = it.get("settleCoin") or margin_asset
                out.append({
                    "exchange": "bybit",
                    "symbol": sym,
                    "base": base,
                    "quote": quote,
                    "contract_type": "perpetual",
                    "margin_asset": margin,
                    "settle_asset": margin,
                    "linear_inverse": linear_inverse,
                    "status": self.norm_status(it.get("status")),
                    "funding_interval_h": 8,
                    "raw": it,
                })
        return out

    # ---------- OKX ----------

    async def okx_pairs(self, client: httpx.AsyncClient) -> List[Dict[str, Any]]:
        """
        OKX: SWAP — все perpetual.
        Важный момент: volCcy24h для деривативов = ОБЪЁМ В БАЗОВОЙ МОНЕТЕ.
        Чтобы получить приблизительный объём в USDT, делаем: volCcy24h * last.
        Дальше фильтруем по > 5_000_000 USDT.
        """
        min_vol = self.MIN_24H_USDT

        # тикеры для объёмов
        url_tickers = "https://www.okx.com/api/v5/market/tickers"
        data_t = await self.fetch_json(client, url_tickers, {"instType": "SWAP"})
        t_list = await self.safe_get(data_t, "data", default=[]) or []

        # instId -> оценка 24h объёма в USDT
        vol_map: Dict[str, float] = {}
        for t in t_list:
            inst_id = t.get("instId")
            if not inst_id:
                continue

            # volCcy24h — количество базовой монеты за 24h для деривативов
            v_raw = t.get("volCcy24h")
            p_raw = t.get("last") or t.get("lastPx")
            try:
                base_vol = float(v_raw) if v_raw is not None else 0.0
            except (TypeError, ValueError):
                base_vol = 0.0
            try:
                last_price = float(p_raw) if p_raw is not None else 0.0
            except (TypeError, ValueError):
                last_price = 0.0

            # оценка объёма в USDT (для любых USD/USDT/USDC-котируемых контрактов это близко к правде)
            vol_usdt = base_vol * last_price
            vol_map[inst_id] = vol_usdt

        # список инструментов
        url = "https://www.okx.com/api/v5/public/instruments"
        params = {"instType": "SWAP"}
        data = await self.fetch_json(client, url, params)
        lst = await self.safe_get(data, "data", default=[]) or []
        out: List[Dict[str, Any]] = []
        for it in lst:
            sym = it.get("instId")  # BTC-USDT-SWAP
            if not sym:
                continue

            vol_usdt = vol_map.get(sym, 0.0)
            if vol_usdt < min_vol:
                # вот тут теперь ZETA-USDT-SWAP и прочий шлак должен отсеиваться
                continue

            base = it.get("uly") or it.get("baseCcy")
            quote = it.get("quoteCcy")
            settle = it.get("settleCcy")
            lin_inv = "linear" if (settle in ("USDT", "USDC", "USD")) else "inverse"
            out.append({
                "exchange": "okx",
                "symbol": sym,
                "base": base,
                "quote": quote,
                "contract_type": "perpetual",
                "margin_asset": settle,
                "settle_asset": settle,
                "linear_inverse": lin_inv,
                "status": self.norm_status(it.get("state")),
                "funding_interval_h": 8,
                "raw": it,
            })
        return out

    # ---------- Bitget ----------

    async def bitget_pairs(self, client: httpx.AsyncClient) -> List[Dict[str, Any]]:
        """
        Bitget: umcbl(USDT-M), dmcbl(coin-M), cmcbl(USDC-M).
        Используем usdtVolume (если есть) как 24h объём в USDT.
        """
        out: List[Dict[str, Any]] = []
        min_vol = self.MIN_24H_USDT

        for productType, mark in [("umcbl", "linear"), ("dmcbl", "inverse"), ("cmcbl", "linear")]:
            # тикеры с объёмами
            url_tickers = "https://api.bitget.com/api/mix/v1/market/tickers"
            data_t = await self.fetch_json(client, url_tickers, {"productType": productType})
            t_list = await self.safe_get(data_t, "data", default=[]) or []
            vol_map: Dict[str, float] = {}
            for t in t_list:
                sym_t = t.get("symbol")
                if not sym_t:
                    continue
                v_raw = t.get("usdtVolume") or t.get("quoteVolume")
                try:
                    vol = float(v_raw) if v_raw is not None else 0.0
                except (TypeError, ValueError):
                    vol = 0.0
                vol_map[sym_t] = vol

            # контракты
            url = "https://api.bitget.com/api/mix/v1/market/contracts"
            data = await self.fetch_json(client, url, {"productType": productType})
            lst = await self.safe_get(data, "data", default=[]) or []
            for it in lst:
                if it.get("supportMarginCoins"):
                    margin = (it["supportMarginCoins"][0]
                              if isinstance(it["supportMarginCoins"], list)
                              else it["supportMarginCoins"])
                else:
                    margin = it.get("marginCoin") or it.get("quoteCoin")

                sym = it.get("symbol") or f"{it.get('baseCoin')}{margin}"
                if not sym:
                    continue

                vol_usdt = vol_map.get(sym, 0.0)
                if vol_usdt < min_vol:
                    continue

                base = it.get("baseCoin")
                quote = it.get("quoteCoin") or margin
                out.append({
                    "exchange": "bitget",
                    "symbol": sym,
                    "base": base,
                    "quote": quote,
                    "contract_type": "perpetual",
                    "margin_asset": margin,
                    "settle_asset": margin,
                    "linear_inverse": mark,
                    "status": self.norm_status(it.get("status") or it.get("symbolStatus")),
                    "funding_interval_h": 8,
                    "raw": it,
                })
        return out

    # ---------- HTX (Huobi) ----------

    async def htx_pairs(self, client: httpx.AsyncClient) -> List[Dict[str, Any]]:
        """
        HTX (Huobi) USDT-маржинальные перпы.
        Оценка 24h объёма в USDT по batch_merged: amount * close.
        Потом фильтр > 5M.
        Coin-margined перпы не берём, чтобы не городить пересчёт.
        """
        out: List[Dict[str, Any]] = []
        min_vol = self.MIN_24H_USDT

        # тикеры linear-swap
        url_ticks = "https://api.hbdm.com/linear-swap-ex/market/detail/batch_merged"
        data_ticks = await self.fetch_json(client, url_ticks, params={})
        ticks = (data_ticks.get("ticks") or data_ticks.get("data") or [])
        vol_map: Dict[str, float] = {}
        for t in ticks:
            cc = t.get("contract_code")
            if not cc:
                continue
            if not str(cc).endswith("USDT"):
                continue
            try:
                amount = float(t.get("amount") or 0.0)
            except (TypeError, ValueError):
                amount = 0.0
            try:
                close = float(t.get("close") or 0.0)
            except (TypeError, ValueError):
                close = 0.0
            vol = amount * close  # оценка объёма в USDT
            vol_map[cc] = vol

        # USDT-margined perpetuals
        url_linear = "https://api.hbdm.com/linear-swap-api/v1/swap_contract_info"
        data_l = await self.fetch_json(client, url_linear)
        lst_l = await self.safe_get(data_l, "data", default=[]) or []
        for it in lst_l:
            status = it.get("contract_status")
            if status != 1:  # 1 = торгуется
                continue

            sym = it.get("contract_code")
            if not sym:
                continue

            vol_usdt = vol_map.get(sym, 0.0)
            if vol_usdt < min_vol:
                continue

            base = sym.split("-")[0]
            out.append({
                "exchange": "htx",
                "symbol": sym,
                "base": base,
                "quote": "USDT",
                "contract_type": "perpetual",
                "margin_asset": "USDT",
                "settle_asset": "USDT",
                "linear_inverse": "linear",
                "status": self.norm_status("trading"),
                "funding_interval_h": 8,
                "raw": it,
            })

        return out

    # ---------- Главный сборщик ----------

    async def collect_all(self) -> List[Dict[str, Any]]:
        async with httpx.AsyncClient(timeout=self.TIMEOUT, headers=self.HEADERS) as client:
            funcs = [
                ("bybit", self.bybit_pairs(client)),
                ("okx", self.okx_pairs(client)),
                ("bitget", self.bitget_pairs(client)),
                ("htx", self.htx_pairs(client)),
            ]
            tasks = [f for _, f in funcs]
            names = [n for n, _ in funcs]
            results = await asyncio.gather(*tasks, return_exceptions=True)

        rows: List[Dict[str, Any]] = []
        for name, res in zip(names, results):
            if isinstance(res, Exception):
                import traceback
                print(f"[{name}] ERROR:\n", "".join(traceback.format_exception(res)))
            else:
                print(f"[{name}] OK: {len(res)} pairs")
                rows.extend(res)
        return rows

    async def to_dataframe(self, rows: List[Dict[str, Any]]) -> pd.DataFrame:
        df = pd.DataFrame(rows)
        if df.empty:
            return df
        df = df.sort_values(["exchange", "symbol"]).reset_index(drop=True)
        if "raw" in df.columns:
            df["raw"] = df["raw"].map(
                lambda x: json.dumps(x, ensure_ascii=False) if isinstance(x, dict) else None
            )
        return df

    async def save_outputs(self, df: pd.DataFrame) -> None:
        csv_path = self.out_path("csv")
        pq_path = self.out_path("parquet")
        df.to_csv(csv_path, index=False, encoding="utf-8")
        df.to_parquet(pq_path, index=False)
        print(f"Saved: {csv_path}")
        print(f"Saved: {pq_path}")
        print("\nCounts by exchange:")
        print(df["exchange"].value_counts())

    async def main(self):
        rows = await self.collect_all()
        df = await self.to_dataframe(rows)
        if df.empty:
            print("No pairs after volume filter.")
            return df
        # Нормализация base/quote, если не удалось
        mask_bq = df["base"].isna() | df["quote"].isna()
        if mask_bq.any():
            for idx in df[mask_bq].index:
                b, q = self.parse_base_quote_from_symbol(df.at[idx, "symbol"])
                if pd.isna(df.at[idx, "base"]) and b:
                    df.at[idx, "base"] = b
                if pd.isna(df.at[idx, "quote"]) and q:
                    df.at[idx, "quote"] = q
        return df


if __name__ == "__main__":
    asyncio.run(Parsing().main())
