# gate_async_futures_mainnet.py
from __future__ import annotations
import re
import os
import time
import json
import hmac
import hashlib
import asyncio
from typing import Optional, Literal, Dict, Any, List, Tuple
from decimal import Decimal, ROUND_DOWN

import httpx
from dotenv import load_dotenv

load_dotenv()

GATE_API_KEY    = os.getenv("GATE_API_KEY", "")
GATE_API_SECRET = os.getenv("GATE_API_SECRET", "")

# ---------- numeric utils ----------
def _d(x) -> Decimal:
    return Decimal(str(x))

def _round_step(value: Decimal, step: Decimal) -> Decimal:
    if step == 0:
        return value
    q = (value / step).to_integral_value(rounding=ROUND_DOWN)
    return q * step

def _trim_decimals(x: Decimal) -> str:
    s = format(x, "f").rstrip("0").rstrip(".")
    return s if s else "0"

# ======================================================
#                GATE.IO Futures (USDT) API v4
# ======================================================
class GateAsyncFuturesClient:
    """
    Асинхронный клиент Gate.io Futures (USDT-settled) без ccxt.

    Подпись:
      SIGN = hex( HMAC_SHA512(secret, METHOD + "\\n" + URL_PATH + "\\n" + QUERY + "\\n" + hex(SHA512(BODY)) + "\\n" + TIMESTAMP) )
    Заголовки: KEY, Timestamp (секунды), SIGN
    """
    def __init__(
        self,
        api_key: str,
        api_secret: str,
        *,
        testnet: bool = False,
        timeout: float = 15.0,
    ):
        if not api_key or not api_secret:
            raise ValueError("GATE_API_KEY / GATE_API_SECRET не заданы")

        self.api_key    = api_key
        self.api_secret = api_secret.encode("utf-8")
        self.base_host  = "https://fx-api-testnet.gateio.ws" if testnet else "https://fx-api.gateio.ws"
        self.base_path  = "/api/v4"
        self._client    = httpx.AsyncClient(base_url=self.base_host, timeout=timeout, headers={
            "Accept": "application/json",
            "Content-Type": "application/json",
        })
        self.settle     = "usdt"   # для USDT-перпетуалов
        self._account_mode_cache: Optional[str] = None  # "single" | "dual"

    # --- контекстный менеджер ---
    async def __aenter__(self): return self
    async def __aexit__(self, exc_type, exc, tb): await self.close()
    async def close(self): await self._client.aclose()

    # ------------ подпись/запросы ------------
    @staticmethod
    def _ts_sec() -> str:
        return str(int(time.time()))

    @staticmethod
    def _canon_query(params: Dict[str, Any]) -> str:
        if not params:
            return ""
        items = [(k, "" if v is None else str(v)) for k, v in params.items() if v is not None]
        items.sort(key=lambda kv: kv[0])
        return "&".join(f"{k}={v}" for k, v in items)

    @staticmethod
    def _sha512_hex(s: str) -> str:
        m = hashlib.sha512()
        m.update(s.encode("utf-8"))
        return m.hexdigest()

    def _sign(self, method: str, url_path: str, query_raw: str, body_raw: str, ts_sec: str) -> str:
        hashed_payload = self._sha512_hex(body_raw or "")
        s = f"{method}\n{url_path}\n{query_raw}\n{hashed_payload}\n{ts_sec}"
        return hmac.new(self.api_secret, s.encode("utf-8"), hashlib.sha512).hexdigest()

    async def _request(self, method: str, path_tail: str, *,
                       query: Optional[Dict[str, Any]] = None,
                       body: Optional[Dict[str, Any]] = None) -> Any:
        url_path = f"{self.base_path}{path_tail}"
        query_raw = self._canon_query(query or {})
        body_raw  = json.dumps(body, separators=(",", ":"), ensure_ascii=False) if body else ""

        ts = self._ts_sec()
        sign = self._sign(method.upper(), url_path, query_raw, body_raw, ts)

        headers = {
            "KEY": self.api_key,
            "Timestamp": ts,
            "SIGN": sign,
        }

        url = url_path if not query_raw else f"{url_path}?{query_raw}"
        r = await self._client.request(method.upper(), url, headers=headers, content=body_raw if body_raw else None)
        try:
            r.raise_for_status()
        except httpx.HTTPStatusError as e:
            try:
                data = r.json()
            except Exception:
                data = r.text
            raise RuntimeError(f"Gate HTTP {r.status_code}: {data}") from e

        data = r.json()
        if isinstance(data, dict) and ("label" in data or "message" in data):
            raise RuntimeError(f"Gate error: {data}")
        return data

    # ------------ аккаунт/режим ------------
    async def _get_account(self) -> Dict[str, Any]:
        # GET /futures/{settle}/accounts
        return await self._request("GET", f"/futures/{self.settle}/accounts")

    async def get_mode(self) -> str:
        """
        Возвращает 'single' или 'dual'. Кешируем для производительности.
        """
        if self._account_mode_cache:
            return self._account_mode_cache
        acc = await self._get_account()
        mode = acc.get("mode", "") or acc.get("position_mode", "")
        mode = mode.lower() if isinstance(mode, str) else "single"
        if mode not in ("single", "dual"):
            mode = "single"
        self._account_mode_cache = mode
        return mode

    # ------------ market data / instruments ------------
    async def get_contract(self, contract: str) -> Dict[str, Any]:
        return await self._request("GET", f"/futures/{self.settle}/contracts/{contract}")

    async def get_ticker(self, contract: str) -> Dict[str, Any]:
        arr = await self._request("GET", f"/futures/{self.settle}/tickers", query={"contract": contract})
        return arr[0] if isinstance(arr, list) and arr else {}

    # ------------ конвертация USDT -> size ------------
    async def usdt_to_size(self, contract: str, usdt_amount: float | str, *, side: Literal["buy", "sell"]) -> int:
        """
        size = floor( USDT / (quanto_multiplier * price) )
        где price — лучшая сторона (ask/bid) либо last/mark.
        size целочисленный; учитываем минимальный size (order_size_min).
        """
        t = await self.get_ticker(contract)
        px_str = (t.get("ask") if side == "buy" else t.get("bid")) or t.get("last") or t.get("mark_price") or "0"
        price = _d(px_str)
        if price <= 0:
            raise RuntimeError(f"Bad price for {contract}: {price}")

        c = await self.get_contract(contract)
        mult = _d(c.get("quanto_multiplier", "0"))
        if mult <= 0:
            mult = _d("1")

        size_min = int(c.get("order_size_min", 1) or 1)
        raw_size = (_d(usdt_amount) / (mult * price))
        # size — целое; округлим вниз
        size_int = int(raw_size.to_integral_value(rounding=ROUND_DOWN))
        if size_int < size_min:
            size_int = size_min
        return size_int

    # ------------ плечо ------------
    async def set_leverage(self, contract: str, leverage: int | str) -> Dict[str, Any]:
        # POST /futures/{settle}/positions/{contract}/leverage?leverage=10
        return await self._request(
            "POST",
            f"/futures/{self.settle}/positions/{contract}/leverage",
            query={"leverage": str(leverage)}
        )

    # ------------ ордера ------------
    async def _create_order(self, *, contract: str, size: int,
                            price: Optional[str], tif: Literal["gtc","ioc"]="ioc",
                            reduce_only: bool=False, close: Optional[bool]=None,
                            text: Optional[str]=None) -> Dict[str, Any]:
        """
        POST /futures/{settle}/orders
        Body: {contract, size, price, tif, reduce_only?, close?, text?}
        - Market: price="0", tif="ioc"
        - Лонг:  size > 0
        - Шорт:  size < 0
        - single-mode close-all: close=true, size=0, price="0"
        """
        body: Dict[str, Any] = {
            "contract": contract,
            "size": int(size),
            "price": "0" if price is None else str(price),
            "tif": tif,
        }
        if reduce_only:
            body["reduce_only"] = True
        if close is True:
            body["close"] = True
            body["size"]  = 0
            body["price"] = "0"
            body["tif"]   = "ioc"
        if text:
            body["text"] = text

        return await self._request("POST", f"/futures/{self.settle}/orders", body=body)

    async def open_long(self, contract: str, size: int, *, price: Optional[str]=None,
                        tif: Literal["gtc","ioc"]="ioc", leverage: Optional[int|str]=None,
                        client_tag: Optional[str]=None) -> Dict[str, Any]:
        if leverage:
            await self.set_leverage(contract, leverage)
        return await self._create_order(contract=contract, size=abs(int(size)), price=price, tif=tif, text=client_tag)

    async def open_short(self, contract: str, size: int, *, price: Optional[str]=None,
                         tif: Literal["gtc","ioc"]="ioc", leverage: Optional[int|str]=None,
                         client_tag: Optional[str]=None) -> Dict[str, Any]:
        if leverage:
            await self.set_leverage(contract, leverage)
        return await self._create_order(contract=contract, size=-abs(int(size)), price=price, tif=tif, text=client_tag)

    # ------------ позиции ------------
    async def _list_positions_raw(self) -> List[Dict[str, Any]]:
        # GET /futures/{settle}/positions
        arr = await self._request("GET", f"/futures/{self.settle}/positions")
        return arr if isinstance(arr, list) else []

    async def _get_position_sizes(self, contract: str) -> Tuple[int, int]:
        """
        Возвращает (long_size, short_size) по контракту в контрактах (целых).
        """
        items = await self._list_positions_raw()
        long_sz = 0
        short_sz = 0
        for p in items:
            if p.get("contract") != contract:
                continue
            sz = int(p.get("size", 0) or 0)
            if sz > 0:
                long_sz = sz
            elif sz < 0:
                short_sz = -sz
        return long_sz, short_sz

    async def get_open_positions(self, *, contract: Optional[str]=None) -> Optional[List[Dict[str, Any]]]:
        items = await self._list_positions_raw()
        out: List[Dict[str, Any]] = []

        for p in items:
            # фильтрация по нужному контракту
            if contract and p.get("contract") != contract:
                continue

            # размер позиции
            sz_raw = p.get("size", 0) or 0
            sz = int(sz_raw)
            if sz == 0:
                continue

            pos_type = "long" if sz > 0 else "short"

            # ---------- извлекаем время открытия ----------
            # Gate.io обычно отдает время как:
            #   "create_time": "1729853073.123"   (секунды, строка)
            #   "create_time_ms": "1729853073123" (миллисек)
            # fallback: "time", "update_time"
            ct = (
                p.get("create_time_ms")
                or p.get("create_time")
                or p.get("time")
                or p.get("update_time")
            )

            opened_at_ms: Optional[int] = None
            opened_at_iso: Optional[str] = None

            if ct is not None:
                # ct может быть строкой "1729853073.123", или "1729853073123", или числом
                # нормализуем в миллисекунды UTC
                try:
                    ct_float = float(ct)
                    if ct_float < 10**12:
                        # это секунды -> в мс
                        opened_at_ms = int(ct_float * 1000.0)
                    else:
                        # уже миллисекунды
                        opened_at_ms = int(ct_float)
                except (TypeError, ValueError):
                    opened_at_ms = None

                if opened_at_ms is not None:
                    # UTC ISO
                    opened_at_iso = (
                        time.strftime(
                            "%Y-%m-%dT%H:%M:%S",
                            time.gmtime(opened_at_ms / 1000.0)
                        )
                        + "+00:00"
                    )

                    # ---------- время в МСК (UTC+3) ----------
                    # сдвигаем UNIX-время на +3 часа

            out.append({
                "opened_at": opened_at_iso,
                "symbol": p.get("contract"),
                "side": pos_type,
                "usdt": p.get("value", "0"),
                "leverage": p.get("leverage", ""),
                "pnl": p.get("unrealised_pnl", "0")
            })

        return out or None


    # ------------ закрытие ВЕСЁМ ------------
    async def close_long_all(self, contract: str, *, client_tag: Optional[str]=None) -> Optional[Dict[str, Any]]:
        """
        Закрыть весь ЛОНГ по контракту.
        - single-mode: один close=true
        - dual-mode: reduce_only маркет-ордер со знаком "-" на размер лонга
        """
        mode = await self.get_mode()
        long_sz, _ = await self._get_position_sizes(contract)
        if long_sz <= 0:
            return None

        if mode == "single":
            try:
                return await self._create_order(contract=contract, size=0, price=None,
                                                tif="ioc", reduce_only=True, close=True, text=client_tag)
            except RuntimeError as e:
                # если биржа вернула, что close запрещён (на случай гонки режима) — fallback в dual-способ
                if "close is not allowed in dual-mode" not in str(e).lower():
                    raise

        # dual-mode (или fallback)
        return await self._create_order(contract=contract, size=-abs(long_sz), price=None,
                                        tif="ioc", reduce_only=True, close=False, text=client_tag)

    async def close_short_all(self, contract: str, *, client_tag: Optional[str]=None) -> Optional[Dict[str, Any]]:
        """
        Закрыть весь ШОРТ по контракту.
        - single-mode: один close=true
        - dual-mode: reduce_only маркет-ордер со знаком "+" на размер шорта
        """
        mode = await self.get_mode()
        _, short_sz = await self._get_position_sizes(contract)
        if short_sz <= 0:
            return None

        if mode == "single":
            try:
                return await self._create_order(contract=contract, size=0, price=None,
                                                tif="ioc", reduce_only=True, close=True, text=client_tag)
            except RuntimeError as e:
                if "close is not allowed in dual-mode" not in str(e).lower():
                    raise

        # dual-mode (или fallback)
        return await self._create_order(contract=contract, size=abs(short_sz), price=None,
                                        tif="ioc", reduce_only=True, close=False, text=client_tag)

    async def close_all_positions(self, symbol: str) -> Dict[str, Optional[Dict[str, Any]]]:
        symbol = re.split("USDT", symbol)[0]
        symbol = symbol + "_USDT"
        r1 = await self.close_long_all(symbol)
        r2 = await self.close_short_all(symbol)
        return {"long_closed": r1, "short_closed": r2}

    # ------------ удобные обёртки с USDT ------------
    async def open_long_usdt(self, symbol: str, usdt_amount: float | str, *,
                              leverage: Optional[int|str]=None, client_tag: Optional[str]=None) -> Dict[str, Any]:
        symbol = re.split("USDT", symbol)[0]
        symbol = symbol + "_USDT"
        size = await self.usdt_to_size(symbol, usdt_amount, side="buy")
        return await self.open_long(symbol, size, price=None, tif="ioc", leverage=leverage, client_tag=client_tag)

    async def open_short_usdt(self, symbol: str, usdt_amount: float | str, *,
                               leverage: Optional[int|str]=None, client_tag: Optional[str]=None) -> Dict[str, Any]:
        symbol = re.split("USDT", symbol)[0]
        symbol = symbol + "_USDT"
        size = await self.usdt_to_size(symbol, usdt_amount, side="sell")
        return await self.open_short(symbol, size, price=None, tif="ioc", leverage=leverage, client_tag=client_tag)


# ---- пример использования ----
async def main():
    contract = "SIGNUSDT"  # формат Gate: BASE_QUOTE
    contract = re.split("USDT", contract)[0]
    contract = contract + "_USDT"
    print(contract)

    async with GateAsyncFuturesClient(GATE_API_KEY, GATE_API_SECRET, testnet=False) as gate:
        # mode = await gate.get_mode(); print("MODE:", mode)

        # # выставить плечо
        # await gate.set_leverage(contract, 10)

        # # # открыть позиции
        # print(await gate.open_long_usdt(contract, 10, leverage=10))
        # await gate.open_short_usdt(contract, 5, leverage=5)

        # получить позиции
        # positions = await gate.get_open_positions(contract=contract)
        # print("OPEN POS:", positions)

        # # закрыть всё (корректно работает и в dual, и в single)
        res = await gate.close_all_positions(contract)
        print("CLOSE ALL:", res)

if __name__ == "__main__":
    asyncio.run(main())
