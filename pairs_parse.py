import asyncio
import json
import os
from datetime import datetime
from typing import Any, Dict, List, Optional

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

# ---------- Утилиты ----------

    async def safe_get(self,d: dict, *path, default=None):
        cur = d
        for k in path:
            if isinstance(cur, dict) and k in cur:
                cur = cur[k]
            else:
                return default
        return cur

    async def parse_base_quote_from_symbol(self,sym: str):
        # На некоторых биржах формат предсказуем (BTCUSDT / ETH-USD-SWAP и т.п.)
        # Делаем простую эвристику — безопасно вернуть None, если не угадали.
        known_quotes = (
            "USDT", "USDC", "USD", "USDⓈ", "BUSD", "BTC", "ETH", "EUR", "TRY", "BRL", "TUSD", "FDUSD", "DAI"
        )
        for q in sorted(known_quotes, key=len, reverse=True):
            if sym.endswith(q):
                return sym[: -len(q)].strip("-_/:"), q
        # форматы типа BTC-USD-SWAP
        if sym.endswith("-SWAP") and "-" in sym:
            parts = sym.split("-")
            if len(parts) >= 3:
                return parts[0], parts[1]
        return None, None

    async def norm_status(self,s) -> str:
        s_low = str(s).lower()
        if any(k in s_low for k in ("trading","listed","online","open","enabled","active","1")):
            return "trading"
        if any(k in s_low for k in ("suspend","offline","halt","delist","closed","0","2")):
            return "suspended"
        return "trading"

    async def out_path(self,ext: str) -> str:
        os.makedirs(r"C:\Users\User\my\py\funding_pars\data", exist_ok=True)
        ts = datetime.utcnow().strftime("%Y%m%d_%H%M")
        return os.path.join("data", f"pairs_{ts}.{ext}")

    # ---------- Парсеры по биржам ----------

    async def fetch_json(self,client: httpx.AsyncClient, url: str, params: dict|None=None, retries: int=3) -> dict:
        last_exc = None
        for i in range(retries):
            try:
                r = await client.get(url, params=params, headers=self.HEADERS)
                r.raise_for_status()
                # иногда MEXC возвращает text/json; парсим аккуратно
                try:
                    return r.json()
                except json.JSONDecodeError:
                    return {"raw_text": r.text}
            except Exception as e:
                last_exc = e
                await asyncio.sleep(0.8 * (i+1))
        raise last_exc

    async def bybit_pairs(self,client: httpx.AsyncClient) -> List[Dict[str, Any]]:
        # Bybit v5: linear (USDT/USDC) + inverse (coin)
        out = []
        for cat, linear_inverse, margin_asset in [
            ("linear", "linear", None),   # USDT/USDC
            ("inverse", "inverse", None), # BTC/ETH и др.
        ]:
            url = "https://api.bybit.com/v5/market/instruments-info"
            params = {"category": cat, "limit": 1000}
            data = await self.fetch_json(client, url, params)
            lst = await self.safe_get(data, "result", "list", default=[]) or []
            for it in lst:
                if it.get("contractType") not in ("LinearPerpetual", "InversePerpetual", "Perpetual"):
                    continue
                sym = it.get("symbol")
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

    async def okx_pairs(self,client: httpx.AsyncClient) -> List[Dict[str, Any]]:
        # OKX: SWAP — все perpetual
        url = "https://www.okx.com/api/v5/public/instruments"
        params = {"instType": "SWAP"}
        data = await self.fetch_json(client, url, params)
        lst = await self.safe_get(data, "data", default=[]) or []
        out = []
        for it in lst:
            sym = it.get("instId")  # BTC-USDT-SWAP
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

    async def bitget_pairs(self,client: httpx.AsyncClient) -> List[Dict[str, Any]]:
        # Bitget: три продукта: umcbl(USDT-M), dmcbl(coin-M), cmcbl(USDC-M)
        out = []
        for productType, mark in [("umcbl", "linear"), ("dmcbl", "inverse"), ("cmcbl", "linear")]:
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

    async def mexc_pairs(self,client: httpx.AsyncClient) -> list[dict]:
        out = []
        urls = [
            "https://futures.mexc.com/api/v1/contract/detail",   # основной
            "https://contract.mexc.com/api/v1/contract/detail",  # запасной
        ]
        data = None
        for u in urls:
            try:
                data = await self.fetch_json(client, u)
                break
            except Exception as e:
                print("MEXC endpoint failed:", u, repr(e))
        if not data:
            raise RuntimeError("MEXC: оба эндпоинта не ответили")

        lst = (data.get("data") or [])
        for it in lst:
            # фильтр статуса мягче — некоторые поля называются иначе
            state = str(it.get("state", "trading")).lower()
            if "delist" in state or "unlist" in state:
                continue
            sym = it.get("symbol")
            if not sym:
                continue
            base = it.get("baseCoin") or sym.rstrip("USDT").rstrip("_USDT")
            quote = it.get("quoteCoin") or "USDT"
            margin = it.get("settleCoin") or quote
            out.append({
                "exchange": "mexc",
                "symbol": sym,
                "base": base,
                "quote": quote,
                "contract_type": "perpetual",
                "margin_asset": margin,
                "settle_asset": margin,
                "linear_inverse": "linear",
                "status": self.norm_status(state),
                "funding_interval_h": 8,
                "raw": it,
            })
        return out

    async def kucoin_futures_pairs(self,client: httpx.AsyncClient) -> list[dict]:
        url = "https://api-futures.kucoin.com/api/v1/contracts/active"
        data = await self.fetch_json(client, url)
        lst = data.get("data") or []
        out = []
        for it in lst:
            # не полагайся только на isActive; иногда он True/False/1/"true"
            is_active = str(it.get("isActive", "true")).lower() in ("1","true","trading","online")
            if not is_active:
                continue
            sym = it.get("symbol") or it.get("name")
            if not sym:
                continue
            base = it.get("baseCurrency") or sym.split("-")[0]
            settle = it.get("settleCurrency") or ("USDT" if not it.get("isInverse") else base)
            quote = it.get("quoteCurrency") or ("USDT" if not it.get("isInverse") else base)
            out.append({
                "exchange": "kucoin_futures",
                "symbol": sym,
                "base": base,
                "quote": quote,
                "contract_type": "perpetual",
                "margin_asset": settle,
                "settle_asset": settle,
                "linear_inverse": "inverse" if str(it.get("isInverse","")).lower() in ("1","true") else "linear",
                "status": "trading",
                "funding_interval_h": 8,
                "raw": it,
            })
        return out

    async def gateio_pairs(self,client: httpx.AsyncClient) -> List[Dict[str, Any]]:
        out = []
        # Gate.io Futures USDT only (coin раздел устарел)
        url = "https://api.gateio.ws/api/v4/futures/usdt/contracts"
        data = await self.fetch_json(client, url)
        if isinstance(data, dict) and "data" in data:
            lst = data["data"]
        else:
            lst = data
        for it in lst:
            if not it.get("name") or it.get("in_delisting"):
                continue
            sym = it["name"]
            base, quote = (sym.split("_", 1) + [None])[:2]
            settle = it.get("quanto_multiplier_currency") or quote
            out.append({
                "exchange": "gate",
                "symbol": sym,
                "base": base,
                "quote": quote,
                "contract_type": "perpetual",
                "margin_asset": settle or quote,
                "settle_asset": settle or quote,
                "linear_inverse": "linear",
                "status": self.norm_status("trading" if it.get("is_open") else "suspended"),
                "funding_interval_h": 8,
                "raw": it,
            })
        return out

    async def htx_pairs(self,client: httpx.AsyncClient) -> List[Dict[str, Any]]:
        # HTX (Huobi). Есть два API: linear-swap (USDT-M) и swap (coin-M)
        out = []
        # USDT-margined
        url_linear = "https://api.hbdm.com/linear-swap-api/v1/swap_contract_info"
        data_l = await self.fetch_json(client, url_linear)
        lst_l = await self.safe_get(data_l, "data", default=[])
        for it in lst_l:
            if it.get("contract_status") in (1, 2):  # 1: торгуется, 2: делистинг/пре-делистинг — оставим 1
                sym = f"{it.get('contract_code')}"
                base = it.get("contract_code", "").split("-")[0]
                out.append({
                    "exchange": "htx",
                    "symbol": sym,
                    "base": base,
                    "quote": "USDT",
                    "contract_type": "perpetual" if it.get("contract_type") == "swap" else "perpetual",
                    "margin_asset": "USDT",
                    "settle_asset": "USDT",
                    "linear_inverse": "linear",
                    "status": self.norm_status("trading" if it.get("contract_status") == 1 else "suspended"),
                    "funding_interval_h": 8,
                    "raw": it,
                })
        # coin-margined
        url_inv = "https://api.hbdm.com/swap-api/v1/swap_contract_info"
        data_i = await self.fetch_json(client, url_inv)
        lst_i = await self.safe_get(data_i, "data", default=[]) or []
        for it in lst_i:
            if it.get("contract_status") in (1, 2):
                sym = it.get("contract_code")
                base = it.get("contract_code", "").split("-")[0]
                out.append({
                    "exchange": "htx",
                    "symbol": sym,
                    "base": base,
                    "quote": base,
                    "contract_type": "perpetual" if it.get("contract_type") == "swap" else "perpetual",
                    "margin_asset": base,
                    "settle_asset": base,
                    "linear_inverse": "inverse",
                    "status": self.norm_status("trading" if it.get("contract_status") == 1 else "suspended"),
                    "funding_interval_h": 8,
                    "raw": it,
                })
        return out

    # ---------- Главный сборщик ----------

    async def collect_all(self) -> list[dict]:
        async with httpx.AsyncClient(timeout=self.TIMEOUT, headers=self.HEADERS) as client:
            funcs = [
                ("bybit", self.bybit_pairs(client)),
                ("okx", self.okx_pairs(client)),
                ("bitget", self.bitget_pairs(client)),
                ("mexc", self.mexc_pairs(client)),
                ("kucoin_futures", self.kucoin_futures_pairs(client)),
                ("gate", self.gateio_pairs(client)),
                ("htx", self.htx_pairs(client)),
            ]
            tasks = [f for _, f in funcs]
            names = [n for n, _ in funcs]
            results = await asyncio.gather(*tasks, return_exceptions=True)

        rows = []
        for name, res in zip(names, results):
            if isinstance(res, Exception):
                import traceback; print(f"[{name}] ERROR:\n", "".join(traceback.format_exception(res)))
            else:
                print(f"[{name}] OK: {len(res)} pairs")
                rows.extend(res)
        return rows

    async def to_dataframe(self,rows: List[Dict[str, Any]]) -> pd.DataFrame:
        df = pd.DataFrame(rows)
        # сортировка для удобства
        df = df.sort_values(["exchange", "symbol"]).reset_index(drop=True)
        # raw в JSON-строку (для удобного экспорта)
        if "raw" in df.columns:
            df["raw"] = df["raw"].map(lambda x: json.dumps(x, ensure_ascii=False) if isinstance(x, dict) else None)
        return df

    async def save_outputs(self,df: pd.DataFrame) -> None:
        csv_path = self.out_path("csv")
        pq_path  = self.out_path("parquet")
        df.to_csv(csv_path, index=False, encoding="utf-8")
        df.to_parquet(pq_path, index=False)
        print(f"Saved: {csv_path}")
        print(f"Saved: {pq_path}")
        # Краткая сводка
        print("\nCounts by exchange:")
        print(df["exchange"].value_counts())

    async def main(self):
        rows = await self.collect_all()
        df = await self.to_dataframe(rows)
        # Нормализация base/quote, если не удалось
        mask_bq = df["base"].isna() | df["quote"].isna()
        if mask_bq.any():
            for idx in df[mask_bq].index:
                b, q = self.parse_base_quote_from_symbol(df.at[idx, "symbol"])
                if pd.isna(df.at[idx, "base"]) and b:
                    df.at[idx, "base"] = b
                if pd.isna(df.at[idx, "quote"]) and q:
                    df.at[idx, "quote"] = q
        return(df)

if __name__ == "__main__":
    asyncio.run(Parsing().main())

