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
#          BITGET MIX V2
# ================================
class BitgetAsyncClient:
    """
    Асинхронный клиент Bitget Mix V2 (USDT-M перпетуалы).

    Основные правила:
      - Все REST-эндпоинты используют каталог /api/v2/mix/...
      - productType всегда 'USDT-FUTURES' для USDT-маржинальных фьючерсов.
      - symbol в запросах V2: 'BTCUSDT' (без '_UMCBL').
        Для удобства клиент принимает:
          • 'BTC/USDT'
          • 'BTCUSDT'
          • 'BTCUSDT_UMCBL'
      - Позиционный режим и маржинальный режим (hedge/one-way, crossed/isolated)
        настраиваются на стороне аккаунта. Здесь по умолчанию marginMode='crossed'.
    """

    def __init__(
        self,
        api_key: str,
        api_secret: str,
        passphrase: str,
        *,
        timeout: float = 15.0,
        base_url: str = "https://api.bitget.com",
        margin_coin: str = "USDT",
        margin_mode: str = "crossed",  # 'crossed' или 'isolated'
    ):
        if not api_key or not api_secret or not passphrase:
            raise ValueError("BITGET_API_KEY / BITGET_API_SECRET / BITGET_API_PASSPHRASE не заданы")

        self.api_key       = api_key
        self.api_secret    = api_secret.encode("utf-8")
        self.passphrase    = passphrase
        self.base_url      = base_url.rstrip("/")
        self.product_type  = "USDT-FUTURES"
        self.margin_coin   = margin_coin.upper()
        self.margin_mode   = margin_mode  # только для place-order
        self._client       = httpx.AsyncClient(base_url=self.base_url, timeout=timeout, verify=SSL_CTX)

    # --- context manager ---
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.close()

    async def close(self):
        await self._client.aclose()

    # --- symbol helpers ---
    @staticmethod
    def spot_like_symbol_to_umcbl(symbol: str) -> str:
        """
        'BIOUSDT' -> 'BIOUSDT_UMCBL'
        Если уже оканчивается на '_UMCBL' — вернёт как есть.

        Нужен только как внутренний формат. Все реальные REST-запросы используют
        V2-формат 'BTCUSDT', который получается через _to_v2_symbol().
        """
        s = symbol.upper().replace("/", "")
        return s if s.endswith("_UMCBL") else f"{s}_UMCBL"

    @staticmethod
    def _to_v2_symbol(sym: str) -> str:
        """
        Приводит символ к виду BTCUSDT для V2 Bitget.

        Принимает:
          - 'LSK/USDT'
          - 'LSKUSDT'
          - 'LSKUSDT_UMCBL'

        Возвращает:
          - 'LSKUSDT'
        """
        s = sym.upper().replace("/", "")
        if "_" in s:
            s = s.split("_")[0]
        return s

    # --- signing helpers ---
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
        ts = _now_ms()
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
        ts = _now_ms()
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
    #        MARKET / INSTRUMENTS
    # =============================
    async def _get_ticker(self, sym: str) -> Dict[str, Any]:
        """
        V2 тикер.

        sym может быть 'LSK/USDT', 'LSKUSDT' или 'LSKUSDT_UMCBL'.
        Возвращает первый элемент из data[0] с полями lastPr, askPr, bidPr, markPrice и т.п.
        """
        symbol_v2 = self._to_v2_symbol(sym)  # -> 'LSKUSDT'
        params = {
            "productType": self.product_type,
            "symbol": symbol_v2,
        }
        raw = await self._get("/api/v2/mix/market/ticker", params)
        rows = raw.get("data") or []
        if not rows:
            raise RuntimeError(f"Bitget: пустой тикер для {symbol_v2}")
        return rows[0]

    async def _get_contracts(self) -> List[Dict[str, Any]]:
        """
        V2 Get Contract Config.
        """
        raw = await self._get(
            "/api/v2/mix/market/contracts",
            {"productType": self.product_type},
        )
        return raw.get("data") or []

    async def _get_contract_info(self, symbol_umcbl: str) -> Dict[str, Any]:
        """
        Возвращает объект контракта для заданного символа (может быть с '_UMCBL').
        """
        target = self._to_v2_symbol(symbol_umcbl)
        items = await self._get_contracts()
        for it in items:
            if (it.get("symbol") or "").upper() == target:
                return it
        raise RuntimeError(f"Instrument not found for {symbol_umcbl}")

    # =============================
    #        PRICE → QTY
    # =============================
    async def usdt_to_qty(
        self,
        symbol: str,
        usdt_amount: float | str,
        *,
        side: Literal["buy", "sell"],
    ) -> str:
        """
        Конвертация USDT → qty по лучшей стороне (ask/bid), c учётом minTradeNum и sizeScale.
        """
        sym_internal = self.spot_like_symbol_to_umcbl(symbol)
        t = await self._get_ticker(sym_internal)

        if side == "buy":
            px_str = t.get("askPr") or t.get("lastPr") or t.get("markPrice")
        else:
            px_str = t.get("bidPr") or t.get("lastPr") or t.get("markPrice")

        price = _d(px_str or "0")
        if price <= 0:
            raise RuntimeError(f"Bad price for {symbol}: {price}")

        info = await self._get_contract_info(sym_internal)
        min_trade = _d(info.get("minTradeNum", "0"))
        size_scale = int(info.get("volumePlace", 0) or 0)
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
    async def _single_position(self, symbol_umcbl: str) -> List[Dict[str, Any]]:
        """
        Возвращает список позиций по одному символу (long/short) через V2 single-position.
        """
        symbol_v2 = self._to_v2_symbol(symbol_umcbl)
        raw = await self._get(
            "/api/v2/mix/position/single-position",
            {
                "symbol": symbol_v2,
                "productType": self.product_type,
                "marginCoin": self.margin_coin,
            },
        )
        data = raw.get("data")
        if data is None:
            return []
        if isinstance(data, list):
            return data
        return [data]

    async def _all_positions_raw(self) -> List[Dict[str, Any]]:
        """
        Возвращает 'сырые' позиции через V2 all-position.
        """
        raw = await self._get(
            "/api/v2/mix/position/all-position",
            {
                "productType": self.product_type,
                "marginCoin": self.margin_coin,
            },
        )
        return raw.get("data") or []

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
        Установка плеча по сторонам через V2 set-leverage.
        Для hedge-режима требуется holdSide: 'long' / 'short'.
        """
        symbol_v2 = self._to_v2_symbol(symbol)

        body_long = {
            "symbol": symbol_v2.lower(),
            "productType": self.product_type,
            "marginCoin": self.margin_coin.lower(),
            "leverage": str(long_leverage),
            "holdSide": "long",
        }
        body_short = {
            "symbol": symbol_v2.lower(),
            "productType": self.product_type,
            "marginCoin": self.margin_coin.lower(),
            "leverage": str(short_leverage),
            "holdSide": "short",
        }

        res1 = await self._post("/api/v2/mix/account/set-leverage", body_long)
        res2 = await self._post("/api/v2/mix/account/set-leverage", body_short)
        return {"long": res1, "short": res2}

    async def set_leverage_both(self, symbol: str, leverage: int | str):
        return await self.set_leverage(symbol, long_leverage=leverage, short_leverage=leverage)

    # =============================
    #            ORDERS
    # =============================
    @staticmethod
    def _map_side_to_v2(
        side: Literal["open_long", "open_short", "close_long", "close_short"]
    ) -> Tuple[str, str]:
        """
        Маппинг старых side ('open_long', ...) на V2-поля (side, tradeSide).
        Для hedge_mode:
          open_long  -> (buy,  open)
          open_short -> (sell, open)
          close_long -> (buy,  close)
          close_short-> (sell, close)
        """
        if side == "open_long":
            return "buy", "open"
        if side == "open_short":
            return "sell", "open"
        if side == "close_long":
            return "buy", "close"
        if side == "close_short":
            return "sell", "close"
        raise ValueError(f"Unknown side: {side}")

    async def _place_order(
        self,
        *,
        symbol_umcbl: str,
        side: Literal["open_long", "open_short", "close_long", "close_short"],
        size: str,
        order_type: Literal["market", "limit"] = "market",
        price: Optional[str] = None,
        client_oid: Optional[str] = None,
        reduce_only: Optional[bool] = None,  # в V2 актуально только для one-way, здесь не используем
    ) -> Dict[str, Any]:
        """
        Унифицированный плейс ордер через V2 /mix/order/place-order.
        """
        symbol_v2 = self._to_v2_symbol(symbol_umcbl)
        v2_side, trade_side = self._map_side_to_v2(side)

        body: Dict[str, Any] = {
            "symbol": symbol_v2,
            "productType": self.product_type,
            "marginMode": self.margin_mode,
            "marginCoin": self.margin_coin,
            "size": size,
            "side": v2_side,
            "tradeSide": trade_side,
            "orderType": order_type,
        }

        if order_type == "limit":
            if not price:
                raise ValueError("price is required for LIMIT orders")
            body["price"] = price
            # V2 использует поле force (timeInForceValue в V1).
            # Для обычного лимитника достаточно GTC:
            body["force"] = "gtc"

        if client_oid:
            body["clientOid"] = client_oid

        # reduceOnly в V2 применяется только в one-way режиме; чтобы не ловить ошибки,
        # просто не передаём его.

        return await self._post("/api/v2/mix/order/place-order", body)

    async def open_long(
        self,
        symbol: str,
        qty: str,
        *,
        order_type: Literal["market", "limit"] = "market",
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
        order_type: Literal["market", "limit"] = "market",
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
        order_type: Literal["market", "limit"] = "market",
        price: Optional[str] = None,
        leverage: Optional[int | str] = None,
        client_oid: Optional[str] = None,
    ) -> Dict[str, Any]:
        qty = await self.usdt_to_qty(symbol, usdt_amount, side="buy")
        return await self.open_long(
            symbol,
            qty,
            order_type=order_type,
            price=price,
            leverage=leverage,
            client_oid=client_oid,
        )

    async def open_short_usdt(
        self,
        symbol: str,
        usdt_amount: float | str,
        *,
        order_type: Literal["market", "limit"] = "market",
        price: Optional[str] = None,
        leverage: Optional[int | str] = None,
        client_oid: Optional[str] = None,
    ) -> Dict[str, Any]:
        qty = await self.usdt_to_qty(symbol, usdt_amount, side="sell")
        return await self.open_short(
            symbol,
            qty,
            order_type=order_type,
            price=price,
            leverage=leverage,
            client_oid=client_oid,
        )

    # =============================
    #        CLOSE FULL SIDE
    # =============================
    async def _get_side_size(
        self,
        symbol_umcbl: str,
        want_side: Literal["long", "short"],
    ) -> Tuple[Decimal, Optional[Dict[str, Any]]]:
        """
        Возвращает (size, raw_position) по стороне, где size — абсолютный объём (в базовой монете).
        """
        items = await self._single_position(symbol_umcbl)
        for it in items:
            hold_side = (it.get("holdSide") or "").lower()
            if hold_side != want_side:
                continue
            for key in ("available", "total"):
                if key in it and _d(it.get(key) or "0") > 0:
                    return _d(it.get(key)), it
        return _d(0), None

    async def _contract_step_info(self, symbol_umcbl: str) -> Tuple[Decimal, Decimal]:
        """
        Возвращает (minTradeNum, step) по volumePlace.
        """
        info = await self._get_contract_info(symbol_umcbl)
        min_trade = _d(info.get("minTradeNum", "0"))
        size_scale = int(info.get("volumePlace", 0) or 0)
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
        Закрывает обе стороны по символу (hedge-режим поддерживается Bitget server-side).
        """
        res_long = await self.close_long_all(symbol)
        res_short = await self.close_short_all(symbol)
        return {"long_closed": res_long, "short_closed": res_short}

    # =============================
    #        OPEN POSITIONS (UI)
    # =============================
    async def get_open_positions(
        self,
        *,
        symbol: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """
        Унифицированная "плоская" информация по одной позиции (первой найденной).

        Если symbol передан, берём первую позицию по этому символу.
        Если нет — берём просто первую позицию из all-position.
        """
        items = await self._all_positions_raw()
        if not items:
            return None

        symbol_v2: Optional[str] = None
        if symbol is not None:
            target = self._to_v2_symbol(symbol)
            for it in items:
                if (it.get("symbol") or "").upper() == target:
                    symbol_v2 = target
                    pos = it
                    break
            else:
                return None
        else:
            pos = items[0]
            symbol_v2 = pos.get("symbol")

        leverage = float(pos.get("leverage") or 0) or 1.0
        open_price = pos.get("openPriceAvg") or pos.get("averageOpenPrice") or "0"
        available = pos.get("available") or pos.get("total") or "0"

        out: Dict[str, Any] = {
            "opened_at": None,
            "symbol": symbol_v2,
            "side": pos.get("holdSide"),
            "leverage": leverage,
            "entry_price": float(open_price),
            "market_price": float(pos.get("markPrice") or pos.get("marketPrice") or 0),
            "liq_price": float(pos.get("liquidationPrice") or 0),
            "pnl": float(pos.get("unrealizedPL") or 0),
        }

        # оценочный вход в USDT = entry_price * size / leverage
        try:
            entry_price_f = float(open_price)
            size_f = float(available)
            out["entry_usdt"] = out["usdt"] = entry_price_f * size_f / leverage
        except Exception:
            out["entry_usdt"] = out["usdt"] = 0.0

        return out

    # =============================
    #        ACCOUNT BALANCE
    # =============================
    async def get_usdt_balance(self) -> str:
        """
        Возвращает общий баланс (equity) в USDT для USDT-маржинальных перпетуалов через V2.
        GET /api/v2/mix/account/accounts?productType=USDT-FUTURES
        """
        data = await self._get(
            "/api/v2/mix/account/accounts",
            {"productType": self.product_type},
        )
        rows = data.get("data") or []
        total = _d("0")
        for r in rows:
            total += _d(
                r.get("usdtEquity")
                or r.get("equity")
                or "0"
            )
        return _trim_decimals(total.normalize())


# ---- Пример использования ----
async def main():
    symbol = "BIOUSDT"  # будет преобразовано в 'BIOUSDT_UMCBL' / 'BIOUSDT'
    async with BitgetAsyncClient(BITGET_API_KEY, BITGET_API_SECRET, BITGET_API_PASSPHRASE) as client:
        print(await client.usdt_to_qty(symbol=symbol, usdt_amount=100, side="long"))

if __name__ == "__main__":
    asyncio.run(main())
