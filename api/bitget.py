# bitget_async_futures_mainnet.py
from __future__ import annotations

import os
import hmac
import time
import json
import base64
import hashlib
import asyncio
import urllib.parse
from typing import Optional, Literal, Dict, Any, List, Tuple

import httpx
from decimal import Decimal, ROUND_DOWN
from datetime import datetime, timezone
import ssl
from dotenv import load_dotenv
load_dotenv()

SSL_CTX = ssl.create_default_context()
BITGET_API_KEY        = os.getenv("BITGET_API_KEY", "")
BITGET_API_SECRET     = os.getenv("BITGET_API_SECRET", "")
BITGET_API_PASSPHRASE = os.getenv("BITGET_API_PASSPHRASE", "")

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

def _now_ms() -> str:
    return str(int(time.time() * 1000))

def _to_iso_ms(ms: str | int | None) -> Optional[str]:
    if not ms:
        return None
    try:
        return datetime.fromtimestamp(int(ms) / 1000, tz=timezone.utc).isoformat()
    except Exception:
        return None

# ================================
#          BITGET MIX (UMCBL)
# ================================
class BitgetAsyncClient:
    """
    Асинхронный клиент Bitget Mix v1 (UMCBL, USDT-марж. перпетуалы) без ccxt.

    Базовые правила Bitget:
      - Подпись: sign = BASE64(HMAC_SHA256(secret, prehash))
      - prehash = timestamp + method + requestPath + (queryString|body)
      - Заголовки: ACCESS-KEY, ACCESS-SIGN, ACCESS-TIMESTAMP, ACCESS-PASSPHRASE, Content-Type
      - symbol для UMCBL: 'BTCUSDT_UMCBL' (т.е. <COIN>USDT_UMCBL)
      - productType: 'umcbl'
      - side для ордеров: 'open_long'|'open_short'|'close_long'|'close_short'
    """

    def __init__(
        self,
        api_key: str,
        api_secret: str,
        passphrase: str,
        *,
        timeout: float = 15.0,
        base_url: str = "https://api.bitget.com",
        product_type: str = "umcbl",        # USDT-margined perpetuals
        margin_coin: str = "USDT",
    ):
        if not api_key or not api_secret or not passphrase:
            raise ValueError("BITGET_API_KEY / BITGET_API_SECRET / BITGET_API_PASSPHRASE не заданы")

        self.api_key       = api_key
        self.api_secret    = api_secret.encode("utf-8")  # raw secret
        self.passphrase    = passphrase
        self.base_url      = base_url.rstrip("/")
        self.product_type  = product_type
        self.margin_coin   = margin_coin
        self._client       = httpx.AsyncClient(base_url=self.base_url, timeout=timeout, verify=SSL_CTX)

    # --- context manager ---
    async def __aenter__(self):
        return self
    async def __aexit__(self, exc_type, exc, tb):
        await self.close()
    async def close(self):
        await self._client.aclose()

    # --- helpers ---
    @staticmethod
    def spot_like_symbol_to_umcbl(symbol: str) -> str:
        """
        'BIOUSDT' -> 'BIOUSDT_UMCBL'
        Если уже оканчивается на '_UMCBL' — вернёт как есть.
        """
        s = symbol.upper()
        return s if s.endswith("_UMCBL") else f"{s}_UMCBL"

    def _sign(self, ts: str, method: str, request_path: str, *, query: str = "", body: str = "") -> str:
        prehash = f"{ts}{method.upper()}{request_path}"
        if method.upper() in ("GET", "DELETE"):
            prehash += query
        else:  # POST
            prehash += body
        digest = hmac.new(self.api_secret, prehash.encode("utf-8"), hashlib.sha256).digest()
        return base64.b64encode(digest).decode()

    def _auth_headers(self, sign: str, ts: str) -> Dict[str, str]:
        return {
            "ACCESS-KEY": self.api_key,
            "ACCESS-SIGN": sign,
            "ACCESS-TIMESTAMP": ts,
            "ACCESS-PASSPHRASE": self.passphrase,
            "Content-Type": "application/json",
        }

    @staticmethod
    def _err(data: Dict[str, Any]) -> str:
        return f"{data.get('code')} - {data.get('msg')}"

    # =============================
    #        LOW-LEVEL HTTP
    # =============================
    async def _get(self, path: str, params: Dict[str, Any]) -> Dict[str, Any]:
        ts = str(int(time.time() * 1000))
        # Bitget хочет queryString в виде '?a=1&b=2' ВКЛЮЧЁННЫМ в строку подписи
        query = ""
        if params:
            qs = urllib.parse.urlencode({k: v for k, v in params.items() if v is not None})
            query = f"?{qs}"
        sign = self._sign(ts, "GET", path, query=query)
        headers = self._auth_headers(sign, ts)
        url = f"{path}{query}"
        r = await self._client.get(url, headers=headers)
        r.raise_for_status()
        data = r.json()
        if data.get("code") not in ("00000", 0, "0"):
            raise RuntimeError(f"Bitget error: {self._err(data)}")
        return data

    async def _post(self, path: str, body: Dict[str, Any]) -> Dict[str, Any]:
        ts = str(int(time.time() * 1000))
        body_json = json.dumps(body, separators=(",", ":"), ensure_ascii=False)
        sign = self._sign(ts, "POST", path, body=body_json)
        headers = self._auth_headers(sign, ts)
        r = await self._client.post(path, headers=headers, content=body_json)
        r.raise_for_status()
        data = r.json()
        if data.get("code") not in ("00000", 0, "0"):
            raise RuntimeError(f"Bitget error: {self._err(data)} | {data}")
        return data

    # =============================
    #        MARKET / INSTR
    # =============================
    async def _get_ticker(self, symbol_umcbl: str) -> Dict[str, Any]:
        data = await self._get("/api/mix/v1/market/ticker", {"symbol": symbol_umcbl})
        return (data.get("data") or {})

    async def _get_contracts(self) -> List[Dict[str, Any]]:
        data = await self._get("/api/mix/v1/market/contracts", {"productType": self.product_type})
        return data.get("data") or []

    async def _get_contract_info(self, symbol_umcbl: str) -> Dict[str, Any]:
        items = await self._get_contracts()
        for it in items:
            if (it.get("symbol") or "").upper() == symbol_umcbl.upper():
                return it
        raise RuntimeError(f"Instrument not found for {symbol_umcbl}")

    async def usdt_to_qty(
        self,
        symbol: str,
        usdt_amount: float | str,
        *,
        side: Literal["buy","sell"],
    ) -> str:
        """
        Конвертация USDT → qty по лучшей стороне (ask/bid), c учётом minTradeNum и sizeScale.
        """
        sym = self.spot_like_symbol_to_umcbl(symbol)
        t = await self._get_ticker(sym)
        px_str = (t.get("askPx") if side == "buy" else t.get("bidPx")) or t.get("last")
        price = _d(px_str or "0")
        if price <= 0:
            raise RuntimeError(f"Bad price for {sym}: {price}")

        info = await self._get_contract_info(sym)
        # minTradeNum — минимальный размер ордера; sizeScale — количество знаков после запятой для size
        min_trade = _d(info.get("minTradeNum", "0"))
        size_scale = int(info.get("sizeScale", 0) or 0)
        step = _d("1") / (_d(10) ** size_scale) if size_scale > 0 else _d("0")

        qty = _d(usdt_amount) / price
        if step > 0:
            qty = _round_step(qty, step)
        if qty < min_trade:
            qty = min_trade
        return _trim_decimals(qty)

    # =============================
    #          POSITIONS RAW
    # =============================
    async def _all_positions(self) -> List[Dict[str, Any]]:
        # Возвращает все позиции по productType
        data = await self._get("/api/mix/v1/position/allPosition", {"productType": self.product_type})
        return data.get("data") or []

    async def _single_position(self, symbol_umcbl: str) -> List[Dict[str, Any]]:
        # Может вернуть две записи: long и short
        data = await self._get(
            "/api/mix/v1/position/singlePosition",
            {"symbol": symbol_umcbl, "marginCoin": self.margin_coin}
        )
        # API иногда возвращает словарь или список — нормализуем к списку
        raw = data.get("data")
        if raw is None:
            return []
        if isinstance(raw, list):
            return raw
        return [raw]

    # =============================
    #          LEVERAGE
    # =============================
    async def set_leverage(
        self,
        symbol: str,
        *,
        long_leverage: int | str,
        short_leverage: int | str,
    ) -> Dict[str, Any]:
        """
        Установка плеча по сторонам. Bitget требует holdSide: 'long'/'short'.
        """
        sym = self.spot_like_symbol_to_umcbl(symbol)

        body_long = {
            "symbol": sym,
            "marginCoin": self.margin_coin,
            "leverage": str(long_leverage),
            "holdSide": "long",
        }
        body_short = {
            "symbol": sym,
            "marginCoin": self.margin_coin,
            "leverage": str(short_leverage),
            "holdSide": "short",
        }
        # Последовательно; игнорируем повторную установку как ошибку
        res1 = await self._post("/api/mix/v1/account/setLeverage", body_long)
        res2 = await self._post("/api/mix/v1/account/setLeverage", body_short)
        return {"long": res1, "short": res2}

    async def set_leverage_both(self, symbol: str, leverage: int | str):
        return await self.set_leverage(symbol, long_leverage=leverage, short_leverage=leverage)

    # =============================
    #            ORDERS
    # =============================
    async def _place_order(
        self,
        *,
        symbol_umcbl: str,
        side: Literal["open_long","open_short","close_long","close_short"],
        size: str,
        order_type: Literal["market","limit"] = "market",
        price: Optional[str] = None,
        client_oid: Optional[str] = None,
        reduce_only: Optional[bool] = None,  # Bitget сам выводит из side, но можно явно
    ) -> Dict[str, Any]:
        body: Dict[str, Any] = {
            "symbol": symbol_umcbl,
            "marginCoin": self.margin_coin,
            "side": side,
            "size": size,
            "orderType": order_type,
            "timeInForceValue": "normal" if order_type == "limit" else "normal",
        }
        if price and order_type == "limit":
            body["price"] = price
        if client_oid:
            body["clientOid"] = client_oid
        if reduce_only is not None:
            body["reduceOnly"] = bool(reduce_only)

        data = await self._post("/api/mix/v1/order/placeOrder", body)
        return data

    async def open_long(
        self,
        symbol: str,
        qty: str,
        *,
        order_type: Literal["market","limit"] = "market",
        price: Optional[str] = None,
        leverage: Optional[int | str] = None,
        client_oid: Optional[str] = None,
    ) -> Dict[str, Any]:
        if leverage:
            await self.set_leverage_both(symbol, leverage)
        sym = self.spot_like_symbol_to_umcbl(symbol)
        return await self._place_order(
            symbol_umcbl=sym,
            side="open_long",
            size=qty,
            order_type=order_type,
            price=price,
            client_oid=client_oid,
            reduce_only=False,
        )

    async def open_short(
        self,
        symbol: str,
        qty: str,
        *,
        order_type: Literal["market","limit"] = "market",
        price: Optional[str] = None,
        leverage: Optional[int | str] = None,
        client_oid: Optional[str] = None,
    ) -> Dict[str, Any]:
        if leverage:
            await self.set_leverage_both(symbol, leverage)
        sym = self.spot_like_symbol_to_umcbl(symbol)
        return await self._place_order(
            symbol_umcbl=sym,
            side="open_short",
            size=qty,
            order_type=order_type,
            price=price,
            client_oid=client_oid,
            reduce_only=False,
        )

    # Удобные обёртки через USDT
    async def open_long_usdt(
        self,
        symbol: str,
        usdt_amount: float | str,
        *,
        order_type: Literal["market","limit"]="market",
        price: Optional[str]=None,
        leverage: Optional[int | str]=None,
        client_oid: Optional[str]=None,
    ) -> Dict[str, Any]:
        qty = await self.usdt_to_qty(symbol, usdt_amount, side="buy")
        return await self.open_long(symbol, qty, order_type=order_type, price=price, leverage=leverage, client_oid=client_oid)

    async def open_short_usdt(
        self,
        symbol: str,
        usdt_amount: float | str,
        *,
        order_type: Literal["market","limit"]="market",
        price: Optional[str]=None,
        leverage: Optional[int | str]=None,
        client_oid: Optional[str]=None,
    ) -> Dict[str, Any]:
        qty = await self.usdt_to_qty(symbol, usdt_amount, side="sell")
        return await self.open_short(symbol, qty, order_type=order_type, price=price, leverage=leverage, client_oid=client_oid)

    # =============================
    #        CLOSE FULL SIDE
    # =============================
    async def _get_side_size(self, symbol_umcbl: str, want_side: Literal["long","short"]) -> Tuple[Decimal, Optional[Dict[str, Any]]]:
        """
        Возвращает (size, raw_position) по стороне, где size — абсолютный объём (в базовой монете).
        """
        items = await self._single_position(symbol_umcbl)
        for it in items:
            hold_side = (it.get("holdSide") or "").lower()  # 'long'|'short'
            if hold_side != want_side:
                continue
            # Bitget поля: total, available, locked, держим абсолют
            # В части аккаунтов есть 'total', в части — 'size'/'holdVol'. Нормализуем:
            for key in ("available", "total", "holdVol", "size"):
                if key in it and _d(it.get(key) or "0") > 0:
                    return _d(it.get(key)), it
        return _d(0), None

    async def _contract_step_info(self, symbol_umcbl: str) -> Tuple[Decimal, Decimal]:
        """
        Возвращает (minTradeNum, step) по sizeScale.
        """
        info = await self._get_contract_info(symbol_umcbl)
        min_trade = _d(info.get("minTradeNum", "0"))
        size_scale = int(info.get("sizeScale", 0) or 0)
        step = _d("1") / (_d(10) ** size_scale) if size_scale > 0 else _d("0")
        return min_trade, step

    async def close_long_all(self, symbol: str) -> Optional[Dict[str, Any]]:
        sym = self.spot_like_symbol_to_umcbl(symbol)
        size, _ = await self._get_side_size(sym, "long")
        if size <= 0:
            return None
        min_trade, step = await self._contract_step_info(sym)
        qty = size
        if step > 0:
            qty = _round_step(qty, step)
        if qty < min_trade:
            qty = min_trade
        return await self._place_order(
            symbol_umcbl=sym,
            side="close_long",
            size=_trim_decimals(qty),
            order_type="market",
            reduce_only=True,
        )

    async def close_short_all(self, symbol: str) -> Optional[Dict[str, Any]]:
        sym = self.spot_like_symbol_to_umcbl(symbol)
        size, _ = await self._get_side_size(sym, "short")
        if size <= 0:
            return None
        min_trade, step = await self._contract_step_info(sym)
        qty = size
        if step > 0:
            qty = _round_step(qty, step)
        if qty < min_trade:
            qty = min_trade
        return await self._place_order(
            symbol_umcbl=sym,
            side="close_short",
            size=_trim_decimals(qty),
            order_type="market",
            reduce_only=True,
        )

    async def close_all_positions(self, symbol: str) -> Dict[str, Optional[Dict[str, Any]]]:
        """
        Закрывает обе стороны по символу (хедж-режим поддерживается server-side).
        """
        res_long  = await self.close_long_all(symbol)
        res_short = await self.close_short_all(symbol)
        return {"long_closed": res_long, "short_closed": res_short}

    # =============================
    #        OPEN POSITIONS (UI)
    # =============================
    async def _all_positions(self) -> List[Dict[str, Any]]:
        """
        Возвращает "сырые" позиции. Пытаемся v1→v1(v2)→v2, т.к. Bitget частично мигрировал на v2.
        """
        # v1 /allPosition
        try:
            d = await self._get("/api/mix/v1/position/allPosition", {"productType": self.product_type, "marginCoin": self.margin_coin})
            rows = d.get("data") or []
            if rows:
                return rows
        except Exception:
            pass

        # v1 /allPosition-v2
        try:
            d = await self._get("/api/mix/v1/position/allPosition-v2", {"productType": self.product_type, "marginCoin": self.margin_coin})
            rows = d.get("data") or []
            if rows:
                return rows
        except Exception:
            pass

        # v2 /all-position (productType меняется на "USDT-FUTURES")
        try:
            d = await self._get("/api/v2/mix/position/all-position", {"productType": "USDT-FUTURES", "marginCoin": self.margin_coin})
            rows = d.get("data") or []
            if rows:
                return rows
        except Exception:
            pass

        return []

    async def get_open_positions(
        self,
        *,
        symbol: Optional[str] = None,
    ) -> Optional[List[Dict[str, Any]]]:
        """
        Унифицированный список открытых позиций.
        """
        items = await self._all_positions()
        if not len(items):
            return None

        out: Dict[str, Any] = {"opened_at": None, 'symbol': None, 'side': None, 'usdt': None, 'leverage': None, 'pnl': None}
        out["opened_at"] = None
        out['symbol'] = symbol
        out['side'] = items[0]["holdSide"]
        out["leverage"] = items[0]["leverage"]
        out['entry_usdt'] = out["usdt"] = float(items[0].get("averageOpenPrice")) * float(items[0].get("available")) / float(out["leverage"])
        out['pnl'] = items[0]["unrealizedPL"]
        out['entry_price'] = items[0]["averageOpenPrice"]
        out['market_price'] =  items[0]['marketPrice']
        out['liq_price'] = items[0]['liquidationPrice']
        return out or None



    async def get_usdt_balance(self) -> str:
        """
        Возвращает общий баланс (equity) в USDT для USDT-маржинальных перпетуалов.

        1) Сначала пробуем новый v2 эндпоинт:
           GET /api/v2/mix/account/accounts?productType=USDT-FUTURES
        2) Если по какой-то причине он недоступен (например, старый региональный кластер),
           откатываемся на старый v1:
           GET /api/mix/v1/account/accounts?productType=umcbl
        """
        # --- пробуем v2 ---
        try:
            data = await self._get(
                "/api/v2/mix/account/accounts",
                {"productType": "USDT-FUTURES"},
            )
        except Exception:
            # --- fallback на v1 ---
            data = await self._get(
                "/api/mix/v1/account/accounts",
                {"productType": self.product_type},  # обычно "umcbl"
            )

        rows = data.get("data") or []
        total = _d("0")
        for r in rows:
            # в ответе есть и usdtEquity, и equity — подстрахуемся
            total += _d(
                r.get("usdtEquity")
                or r.get("equity")
                or "0"
            )
        return _trim_decimals(total.normalize())




# # ---- Пример использования ----
async def main():
    symbol = "BIOUSDT"  # будет преобразовано в 'BIOUSDT_UMCBL'
    async with BitgetAsyncClient(BITGET_API_KEY, BITGET_API_SECRET, BITGET_API_PASSPHRASE) as client:
        # Примеры открытия:
        # print(await client.open_long(symbol="SWARMSUSDT", qty='1000', leverage=5))
        # print(await client.open_short(symbol=symbol, qty=100, leverage=5))
        # print(await client.open_short_usdt(symbol, 20, leverage=5))
        
        # Позиции
        # positions = await client.get_open_positions(symbol="SOONUSDT")
        # print("OPEN POSITIONS:", positions)
        # positions = await client._all_positions()
        # print(positions)

        # r = await asyncio.gather(client.get_open_positions(symbol=symbol), client.close_all_positions(symbol))
        # print(r)
        # Закрыть и лонг, и шорт целиком (если есть)
        # res = await client.close_all_positions("SOONUSDT")
        # print("CLOSE ALL:", res)

        # print(await client.usdt_to_qty(symbol="BIOUSDT", usdt_amount=60, side="buy"))
        print(await client.get_usdt_balance())

if __name__ == "__main__":
    asyncio.run(main())