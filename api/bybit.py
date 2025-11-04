# bybit_async_futures_mainnet.py
from __future__ import annotations

import os
import hmac
import time
import json
import uuid
import hashlib
import asyncio
import urllib.parse
from typing import Optional, Literal, Dict, Any, List

import httpx
from decimal import Decimal, ROUND_DOWN
from datetime import datetime, timezone

from dotenv import load_dotenv
load_dotenv()

API_KEY    = os.getenv("BYBIT_API_KEY", "")
API_SECRET = os.getenv("BYBIT_API_SECRET", "")

# ---------- Вспомогательные числовые утилиты ----------
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
#                    КЛИЕНТ BYBIT v5
# ======================================================
class BybitAsyncClient:
    """
    Асинхронный клиент Bybit v5 без ccxt.
    Подпись приватных методов (SIGN TYPE 2):
      sign = SHA256(timestamp + apiKey + recvWindow + (body_json|raw_query))
    """
    def __init__(
        self,
        api_key: str,
        api_secret: str,
        *,
        testnet: bool = False,
        recv_window_ms: int = 25000,
        timeout: float = 15.0,
    ):
        if not api_key or not api_secret:
            raise ValueError("BYBIT_API_KEY / BYBIT_API_SECRET не заданы")

        self.api_key     = api_key
        self.api_secret  = api_secret.encode("utf-8")
        self.recv_window = str(recv_window_ms)
        self.base_url    = "https://api.bybit.com" if not testnet else "https://api-testnet.bybit.com"
        self._client     = httpx.AsyncClient(base_url=self.base_url, timeout=timeout)

    # --- контекстный менеджер ---
    async def __aenter__(self):
        return self
    async def __aexit__(self, exc_type, exc, tb):
        await self.close()
    async def close(self):
        await self._client.aclose()

    # ------------ ВНУТРЕННИЕ УТИЛИТЫ ------------
    @staticmethod
    def _ts_ms() -> str:
        return str(int(time.time() * 1000))

    @staticmethod
    def _to_iso_ms(ms: str | int | None) -> Optional[str]:
        if not ms:
            return None
        try:
            return datetime.fromtimestamp(int(ms) / 1000, tz=timezone.utc).isoformat()
        except Exception:
            return None

    @staticmethod
    def _err(data: Dict[str, Any]) -> str:
        return f"{data.get('retCode')} - {data.get('retMsg')} | {data.get('retExtInfo') or {}}"

    # --- канонизация query для подписи и URL ---
    @staticmethod
    def _canonical_query(params: Dict[str, Any]) -> tuple[str, str]:
        items = [(k, "" if v is None else str(v)) for k, v in params.items() if v is not None]
        items.sort(key=lambda kv: kv[0])
        raw_query = "&".join(f"{k}={v}" for k, v in items)                   # для подписи БЕЗ url-эскейпа
        url_query = urllib.parse.urlencode(items, doseq=False, safe=":")     # для реального URL
        return raw_query, url_query

    # --- подписи ---
    def _sign_body(self, timestamp: str, body_dict: Dict[str, Any]) -> str:
        body_json = json.dumps(body_dict, separators=(",", ":"), ensure_ascii=False)
        presign = f"{timestamp}{self.api_key}{self.recv_window}{body_json}"
        return hmac.new(self.api_secret, presign.encode("utf-8"), hashlib.sha256).hexdigest()

    def _sign_query(self, timestamp: str, raw_query: str) -> str:
        presign = f"{timestamp}{self.api_key}{self.recv_window}{raw_query}"
        return hmac.new(self.api_secret, presign.encode("utf-8"), hashlib.sha256).hexdigest()

    # --- приватные запросы ---
    async def _post_private(self, path: str, body: Dict[str, Any]) -> Dict[str, Any]:
        ts   = self._ts_ms()
        sign = self._sign_body(ts, body)
        headers = {
            "X-BAPI-API-KEY": self.api_key,
            "X-BAPI-TIMESTAMP": ts,
            "X-BAPI-RECV-WINDOW": self.recv_window,
            "X-BAPI-SIGN-TYPE": "2",
            "X-BAPI-SIGN": sign,
            "Content-Type": "application/json",
        }
        r = await self._client.post(path, headers=headers, content=json.dumps(body, separators=(",", ":")))
        r.raise_for_status()
        data = r.json()
        # 110043: leverage not modified — считаем успехом для set-leverage
        if data.get("retCode") not in (0, 110043):
            raise RuntimeError(f"Bybit error: {self._err(data)}")
        return data

    async def _get_private(self, path: str, params: Dict[str, Any]) -> Dict[str, Any]:
        ts = self._ts_ms()
        raw_query, url_query = self._canonical_query(params)
        sign = self._sign_query(ts, raw_query)
        headers = {
            "X-BAPI-API-KEY": self.api_key,
            "X-BAPI-TIMESTAMP": ts,
            "X-BAPI-RECV-WINDOW": self.recv_window,
            "X-BAPI-SIGN-TYPE": "2",
            "X-BAPI-SIGN": sign,
        }
        url = f"{path}?{url_query}" if url_query else path
        r = await self._client.get(url, headers=headers)  # без params=, чтобы подпись совпадала с URL
        r.raise_for_status()
        data = r.json()
        if data.get("retCode") != 0:
            raise RuntimeError(f"Bybit error: {self._err(data)}")
        return data

    # ------------ ПУБЛИЧНЫЕ МЕТОДЫ (цена/инструмент) ------------
    async def _get_ticker(self, symbol: str, category: Literal["linear","inverse"]="linear") -> Dict[str, Any]:
        r = await self._client.get("/v5/market/tickers", params={"category": category, "symbol": symbol})
        r.raise_for_status()
        data = r.json()
        if data.get("retCode") != 0 or not data.get("result", {}).get("list"):
            raise RuntimeError(f"Ticker error: {self._err(data)}")
        return data["result"]["list"][0]

    async def _get_instrument(self, symbol: str, category: Literal["linear","inverse"]="linear") -> Dict[str, Any]:
        r = await self._client.get("/v5/market/instruments-info", params={"category": category, "symbol": symbol})
        r.raise_for_status()
        data = r.json()
        if data.get("retCode") != 0 or not data.get("result", {}).get("list"):
            raise RuntimeError(f"Instrument error: {self._err(data)}")
        return data["result"]["list"][0]

    async def usdt_to_qty(
        self,
        symbol: str,
        usdt_amount: float | str,
        *,
        side: Literal["buy","sell"],
        category: Literal["linear","inverse"] = "linear",
        leverage: float | str = 1,
    ) -> str:
        """
        Конвертирует USDT → qty (КОНТРАКТЫ) с учётом цены, contractSize, minOrderQty, qtyStep и плеча.
        Если итоговое qty < minOrderQty — возвращает minOrderQty.
        """
        # 1) Цена
        t = await self._get_ticker(symbol, category)
        px_str = t.get("ask1Price") if side == "buy" else t.get("bid1Price")
        if not px_str or _d(px_str) == 0:
            px_str = t.get("lastPrice") or t.get("markPrice")
        price = _d(px_str)
        if price <= 0:
            raise RuntimeError(f"Bad price for {symbol}: {price}")

        # 2) Спека инструмента
        ins = await self._get_instrument(symbol, category)
        lot = ins.get("lotSizeFilter", {}) or {}
        min_qty     = _d(lot.get("minOrderQty", "0"))
        qty_step    = _d(lot.get("qtyStep", "0"))
        contract_sz = _d(ins.get("contractSize", "1"))

        # 3) Нотация (с плечом!)
        usdt = _d(usdt_amount)
        lev  = _d(leverage)
        notional = usdt * lev  # <<< ВАЖНО: учитываем плечо

        # 4) Пересчёт в КОНТРАКТЫ
            # contractSize в coin/contract
        coin_qty      = notional / price
        raw_contracts = coin_qty / (contract_sz if contract_sz > 0 else _d(1))

        # 5) Приведение к шагу/минимуму
        qty = raw_contracts
        if qty_step > 0:
            qty = _round_step(qty, qty_step)
        if qty < min_qty:
            qty = min_qty

        # 6) Возвращаем со строго нужным числом знаков (по шагу)
        if qty_step > 0:
            # форматируем по количеству десятичных знаков шага
            scale = -qty_step.as_tuple().exponent if qty_step != 0 else 0
            return f"{qty:.{max(scale,0)}f}"
        return _trim_decimals(qty)


    # ------------ РАБОТА С ПЛЕЧОМ ------------
    async def set_leverage(
        self,
        symbol: str,
        *,
        buy_leverage: int | str,
        sell_leverage: int | str,
        category: Literal["linear","inverse"]="linear",
    ) -> Dict[str, Any]:
        """
        Устанавливает плечо для символа. 110043 (leverage not modified) — не ошибка.
        """
        body = {
            "category": category,
            "symbol": symbol,
            "buyLeverage": str(buy_leverage),
            "sellLeverage": str(sell_leverage),
        }
        return await self._post_private("/v5/position/set-leverage", body)

    async def set_leverage_both(self, symbol: str, leverage: int | str, *, category: Literal["linear","inverse"]="linear"):
        return await self.set_leverage(symbol, buy_leverage=leverage, sell_leverage=leverage, category=category)

    # ------------ ОРДЕРА (qty — строка) ------------
    async def open_long(
        self,
        symbol: str,
        qty: str,
        *,
        category: Literal["linear","inverse"]="linear",
        order_type: Literal["Market","Limit"]="Market",
        price: Optional[str]=None,
        position_idx: int = 0,
        leverage: Optional[int | str] = None,
        order_link_id: Optional[str]=None,
    ) -> Dict[str, Any]:
        if leverage:
            await self.set_leverage_both(symbol, leverage, category=category)

        if order_link_id is None:
            order_link_id = f"open_long_{uuid.uuid4().hex[:12]}"
        body: Dict[str, Any] = {
            "category": category,
            "symbol": symbol,
            "side": "Buy",
            "orderType": order_type,
            "qty": qty,
            "reduceOnly": False,
            "positionIdx": position_idx,
            "timeInForce": "IOC" if order_type == "Market" else "GTC",
            "orderLinkId": order_link_id,
        }
        if order_type == "Limit":
            if not price:
                raise ValueError("price is required for Limit orders")
            body["price"] = price
        return await self._post_private("/v5/order/create", body)

    async def open_short(
        self,
        symbol: str,
        qty: str,
        *,
        category: Literal["linear","inverse"]="linear",
        order_type: Literal["Market","Limit"]="Market",
        price: Optional[str]=None,
        position_idx: int = 0,
        leverage: Optional[int | str] = None,
        order_link_id: Optional[str]=None,
    ) -> Dict[str, Any]:
        if leverage:
            await self.set_leverage_both(symbol, leverage, category=category)

        if order_link_id is None:
            order_link_id = f"open_short_{uuid.uuid4().hex[:12]}"
        body: Dict[str, Any] = {
            "category": category,
            "symbol": symbol,
            "side": "Sell",
            "orderType": order_type,
            "qty": qty,
            "reduceOnly": False,
            "positionIdx": position_idx,
            "timeInForce": "IOC" if order_type == "Market" else "GTC",
            "orderLinkId": order_link_id,
        }
        if order_type == "Limit":
            if not price:
                raise ValueError("price is required for Limit orders")
            body["price"] = price
        return await self._post_private("/v5/order/create", body)

    # ------------ ВСПОМОГАТЕЛЬНОЕ: ПОЛУЧИТЬ ПОЗИЦИЮ ------------
    async def _get_positions_raw(
        self,
        *,
        symbol: Optional[str] = None,
        category: Literal["linear","inverse"] = "linear",
        settle_coin: str = "USDT",
    ) -> List[Dict[str, Any]]:
        params: Dict[str, Any] = {"category": category, "settleCoin": settle_coin, "limit": 200}
        if symbol:
            params["symbol"] = symbol
        data = await self._get_private("/v5/position/list", params)
        return (data.get("result", {}) or {}).get("list", []) or []

    async def _get_instrument_lot_filters(self, symbol: str, category: Literal["linear","inverse"]) -> tuple[Decimal, Decimal]:
        ins = await self._get_instrument(symbol, category)
        lot = ins.get("lotSizeFilter", {}) or {}
        min_qty  = _d(lot.get("minOrderQty", "0"))
        qty_step = _d(lot.get("qtyStep", "0"))
        return min_qty, qty_step

    # Возвращает (size, positionIdx, side_str) для нужной стороны; size==0 если нет
    async def _find_side_position(
        self,
        symbol: str,
        *,
        want_side: Literal["buy","sell"],
        category: Literal["linear","inverse"]="linear",
        settle_coin: str = "USDT",
    ) -> tuple[Decimal, int, str]:
        items = await self._get_positions_raw(symbol=symbol, category=category, settle_coin=settle_coin)
        best = None
        for it in items:
            if (it.get("symbol") or "") != symbol:
                continue
            side = (it.get("side") or "").lower()  # 'buy' | 'sell'
            if side != want_side:
                continue
            size = _d(it.get("size", "0"))
            idx  = int(it.get("positionIdx", 0) or 0)
            best = (size, idx, side)
            break
        if not best:
            return _d(0), 0, want_side
        return best

   # ------------ ЗАКРЫТИЕ ПОЗИЦИЙ (ВСЕЙ) ------------
    async def close_long_all(
        self,
        symbol: str,
        *,
        category: Literal["linear","inverse"]="linear",
        position_idx: Optional[int] = None,
        order_link_id: Optional[str]=None,
    ) -> Optional[Dict[str, Any]]:
        size, pos_idx_found, _ = await self._find_side_position(symbol, want_side="buy", category=category)
        if size <= 0:
            return None

        if order_link_id is None:
            order_link_id = f"close_long_all_{uuid.uuid4().hex[:10]}"

        body: Dict[str, Any] = {
            "category": category,
            "symbol": symbol,
            "side": "Sell",
            "orderType": "Market",
            # ключевой момент: qty="0" + reduceOnly + closeOnTrigger
            "qty": "0",
            "reduceOnly": True,
            "closeOnTrigger": True,
            # Market-ордер всё равно будет IOC, это ок
            "timeInForce": "IOC",
            "orderLinkId": order_link_id,
            "positionIdx": position_idx if position_idx is not None else pos_idx_found,
            # необязательно, но помогает избежать частичного из-за price-protection:
            "slippageToleranceType": "Percent",
            "slippageTolerance": "5"  # до 5% коридор; при желании сделайте параметром
        }
        return await self._post_private("/v5/order/create", body)

    async def close_short_all(
        self,
        symbol: str,
        *,
        category: Literal["linear","inverse"]="linear",
        position_idx: Optional[int] = None,
        order_link_id: Optional[str]=None,
    ) -> Optional[Dict[str, Any]]:
        size, pos_idx_found, _ = await self._find_side_position(symbol, want_side="sell", category=category)
        if size <= 0:
            return None

        if order_link_id is None:
            order_link_id = f"close_short_all_{uuid.uuid4().hex[:10]}"

        body: Dict[str, Any] = {
            "category": category,
            "symbol": symbol,
            "side": "Buy",
            "orderType": "Market",
            "qty": "0",
            "reduceOnly": True,
            "closeOnTrigger": True,
            "timeInForce": "IOC",
            "orderLinkId": order_link_id,
            "positionIdx": position_idx if position_idx is not None else pos_idx_found,
            "slippageToleranceType": "Percent",
            "slippageTolerance": "5",
        }
        return await self._post_private("/v5/order/create", body)


    async def close_all_positions(
        self,
        symbol: str,
        *,
        category: Literal["linear","inverse"]="linear",
    ) -> Dict[str, Optional[Dict[str, Any]]]:
        """
        Закрывает обе стороны по символу (в хедж-режиме может быть и Buy, и Sell).
        Возвращает словарь с результатами.
        """
        res_long  = await self.close_long_all(symbol, category=category)
        res_short = await self.close_short_all(symbol, category=category)
        return {"long_closed": res_long, "short_closed": res_short}

    # ------------ УДОБНЫЕ ОБЁРТКИ С USDT (ТОЛЬКО ОТКРЫТИЕ) ------------
    async def open_long_usdt(
        self,
        symbol: str,
        usdt_amount: float | str,
        *,
        category: Literal["linear","inverse"]="linear",
        order_type: Literal["Market","Limit"]="Market",
        price: Optional[str]=None,
        position_idx: int = 0,
        leverage: Optional[int | str] = None,
        order_link_id: Optional[str]=None,
    ):
        qty = await self.usdt_to_qty(symbol, usdt_amount, side="buy", category=category)
        return await self.open_long(
            symbol, qty, category=category, order_type=order_type, price=price,
            position_idx=position_idx, leverage=leverage, order_link_id=order_link_id
        )

    async def open_short_usdt(
        self,
        symbol: str,
        usdt_amount: float | str,
        *,
        category: Literal["linear","inverse"]="linear",
        order_type: Literal["Market","Limit"]="Market",
        price: Optional[str]=None,
        position_idx: int = 0,
        leverage: Optional[int | str] = None,
        order_link_id: Optional[str]=None,
    ):
        qty = await self.usdt_to_qty(symbol, usdt_amount, side="sell", category=category)
        return await self.open_short(
            symbol, qty, category=category, order_type=order_type, price=price,
            position_idx=position_idx, leverage=leverage, order_link_id=order_link_id
        )

    # ------------ ОТКРЫТЫЕ ПОЗИЦИИ (УДОБНЫЙ ВЫВОД) ------------
    async def get_open_positions(
        self,
        *,
        symbol: Optional[str] = None,
        category: Literal["linear","inverse"] = "linear",
        settle_coin: str = "USDT",
    ) -> Optional[List[Dict[str, Any]]]:
        """
        Возвращает список открытых позиций:
        {
            "opened_at": ISO-UTC когда позиция впервые была открыта (createdTime),
            "last_update_at": ISO-UTC последнего изменения позиции (updatedTime),
            "coin": "BIO",
            "type": "long"|"short",
            "usdt": "123.45",
            "leverage": "5",
            "pnl": "3.21"
        }
        Если позиций нет — None.
        """
        items = await self._get_positions_raw(symbol=symbol, category=category, settle_coin=settle_coin)
        out: List[Dict[str, Any]] = []

        for it in items:
            size = _d(it.get("size", "0"))
            if size == 0:
                continue

            sym = it.get("symbol", "") or ""
            coin = sym
            for q in ("USDT", "USDC", "USD"):
                if sym.endswith(q):
                    coin = sym[:-len(q)]
                    break

            side = (it.get("side") or "").lower()  # 'buy'/'sell'
            pos_type = "long" if side == "buy" else "short"

            # берём только createdTime как момент входа
            updated_ms = it.get("updatedTime")

            last_upd_iso  = self._to_iso_ms(updated_ms)       # полезно для дебага

            # посчитаем позиционную стоимость в USDT (если биржа не дала готовую)
            position_value = it.get("positionValue")
            if not position_value:
                mark = it.get("markPrice") or it.get("avgPrice") or "0"
                try:
                    position_value = _trim_decimals((_d(mark) * size))
                except Exception:
                    position_value = "0"

            out.append({
                "opened_at": last_upd_iso,
                "symbol": coin,
                "side": pos_type,
                "usdt": position_value,
                "leverage": it.get("leverage", ""),
                "pnl": it.get("unrealisedPnl", "0"),
            })

        return out or None



# ---- Пример использования ----
async def main():
    symbol = "CROUSDT"
    async with BybitAsyncClient(API_KEY, API_SECRET, testnet=False) as client:
        # Открыть для примера
        # print(await client.open_long_usdt(symbol, 10, leverage=5))
        print(await client.open_short_usdt(symbol, 20, leverage=1))

        # Посмотреть открытые позиции
        # positions = await client.get_open_positions(symbol=symbol)
        # print("OPEN POSITIONS:", positions)

        # Закрыть ВЕСЬ лонг и ВЕСЬ шорт (если есть)
        # res = await client.close_all_positions(symbol)
        # print("CLOSE ALL:", res)

        # Или по отдельности:
        # await client.close_long_all(symbol)
        # await client.close_short_all(symbol)

if __name__ == "__main__":
    asyncio.run(main())
