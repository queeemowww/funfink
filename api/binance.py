# binance_async_futures_mainnet.py
from __future__ import annotations

import os
import hmac
import time
import json
import hashlib
import asyncio
import urllib.parse
from typing import Optional, Literal, Dict, Any, List, Tuple

import httpx
from decimal import Decimal, ROUND_DOWN, ROUND_UP  # <= добавь ROUND_UP
from datetime import datetime, timezone
import ssl
from dotenv import load_dotenv
load_dotenv()

SSL_CTX = ssl.create_default_context()
BINANCE_API_KEY    = os.getenv("BINANCE_API_KEY", "")
BINANCE_API_SECRET = os.getenv("BINANCE_API_SECRET", "")

# ---------- numeric utils ----------
def _d(x) -> Decimal:
    return Decimal(str(x))

def _round_step(value: Decimal, step: Decimal) -> Decimal:
    if step == 0:
        return value
    q = (value / step).to_integral_value(rounding=ROUND_DOWN)
    return q * step



def _round_step_up(value: Decimal, step: Decimal) -> Decimal:
    if step == 0:
        return value
    q = (value / step).to_integral_value(rounding=ROUND_UP)
    return q * step


def _trim_decimals(x: Decimal) -> str:
    s = format(x, "f").rstrip(".")
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
#      BINANCE USDⓈ-M FUTURES
# ================================
class BinanceAsyncFuturesClient:
    """
    Асинхронный клиент Binance USDT-M Futures (USDⓈ-M) без ccxt.

    Базовые правила Binance:
      - Подпись: sign = HEX(HMAC_SHA256(secret, queryString))
      - Все приватные запросы подписываются параметрами ?timestamp=...&recvWindow=...&signature=...
      - Заголовок: X-MBX-APIKEY
      - Символы фьючерсов: 'BTCUSDT', 'ETHUSDT', 'BIOUSDT', и т.п.
    """

    def __init__(
        self,
        api_key: str,
        api_secret: str,
        *,
        timeout: float = 15.0,
        base_url: str = "https://fapi.binance.com",
        recv_window: int = 5000,
    ):
        if not api_key or not api_secret:
            raise ValueError("BINANCE_API_KEY / BINANCE_API_SECRET не заданы")

        self.api_key     = api_key
        self.api_secret  = api_secret.encode("utf-8")
        self.base_url    = base_url.rstrip("/")
        self.recv_window = recv_window
        self._client     = httpx.AsyncClient(base_url=self.base_url, timeout=timeout, verify=SSL_CTX)

        # Кэш информации по символам, чтобы не дёргать exchangeInfo каждый раз
        self._symbol_info_cache: Dict[str, Dict[str, Any]] = {}

    # --- context manager ---
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.close()

    async def close(self):
        await self._client.aclose()

    async def _symbol_qty_step_info(self, symbol: str, *, market: bool) -> Tuple[Decimal, Decimal]:
        """
        Возвращает (min_qty, step) для qty.
        Для MARKET стараемся взять MARKET_LOT_SIZE, иначе LOT_SIZE.
        """
        info = await self._get_symbol_info(symbol)
        filters = info.get("filters") or []

        primary = "MARKET_LOT_SIZE" if market else "LOT_SIZE"
        fallback = "LOT_SIZE"

        def _extract(ftype: str) -> Optional[Tuple[Decimal, Decimal]]:
            for f in filters:
                if f.get("filterType") == ftype:
                    return _d(f.get("minQty", "0")), _d(f.get("stepSize", "0"))
            return None

        got = _extract(primary)
        if got is not None:
            return got

        got = _extract(fallback)
        if got is not None:
            return got

        return _d("0"), _d("0")

    # --- helpers ---
    @staticmethod
    def spot_like_symbol(symbol: str) -> str:
        """
        Нормализуем символ к формату Binance USDT-M фьючерсов:
        'bioUsdt' -> 'BIOUSDT'
        """
        return symbol.replace("/", "").upper()

    def _sign(self, query: str) -> str:
        digest = hmac.new(self.api_secret, query.encode("utf-8"), hashlib.sha256).hexdigest()
        return digest

    def _auth_headers(self) -> Dict[str, str]:
        return {"X-MBX-APIKEY": self.api_key}

    @staticmethod
    def _maybe_raise_binance_error(data: Any) -> None:
        """
        Для юзер-эндпоинтов Binance при ошибке обычно приходит {"code": -XXXX, "msg": "..."}.
        """
        if isinstance(data, dict) and "code" in data and data["code"] not in (0, 200):
            raise RuntimeError(f"Binance error: {data['code']} - {data.get('msg')}")

    # =============================
    #        LOW-LEVEL HTTP
    # =============================
    async def _public_get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Any:
        params = {k: v for k, v in (params or {}).items() if v is not None}
        if params:
            qs = urllib.parse.urlencode(params, doseq=True)
            url = f"{path}?{qs}"
        else:
            url = path
        r = await self._client.get(url)
        r.raise_for_status()
        return r.json()
    
    async def _get_mark_price(self, symbol: str) -> Decimal:
        """
        Возвращает markPrice из /fapi/v1/premiumIndex или 0, если не удалось.
        """
        sym = self.spot_like_symbol(symbol)
        try:
            data = await self._public_get("/fapi/v1/premiumIndex", {"symbol": sym})
            return _d(data.get("markPrice") or "0")
        except Exception:
            return _d("0")

    async def _get_last_price(self, symbol: str) -> Decimal:
        """
        Возвращает последнюю цену из /fapi/v1/ticker/price или 0.
        """
        sym = self.spot_like_symbol(symbol)
        try:
            data = await self._public_get("/fapi/v1/ticker/price", {"symbol": sym})
            return _d(data.get("price") or "0")
        except Exception:
            return _d("0")


    async def _signed_request(self, method: str, path: str, params: Optional[Dict[str, Any]] = None) -> Any:
        import httpx  # убедись, что httpx импортирован вверху файла

        params = {k: v for k, v in (params or {}).items() if v is not None}
        params["timestamp"] = int(time.time() * 1000)
        params["recvWindow"] = self.recv_window

        qs = urllib.parse.urlencode(params, doseq=True)
        signature = self._sign(qs)
        qs_with_sig = f"{qs}&signature={signature}"

        url = f"{path}?{qs_with_sig}"
        headers = self._auth_headers()

        method_upper = method.upper()
        if method_upper == "GET":
            r = await self._client.get(url, headers=headers)
        elif method_upper == "POST":
            r = await self._client.post(url, headers=headers)
        elif method_upper == "DELETE":
            r = await self._client.delete(url, headers=headers)
        else:
            raise ValueError(f"Unsupported method {method}")

        text = r.text
        try:
            data = r.json()
        except Exception:
            data = None

        # Если HTTP-ошибка – сразу покажем, что вернул Binance
        if r.status_code >= 400:
            if isinstance(data, dict) and "code" in data:
                print("BINANCE ERROR RAW:", data)
                raise RuntimeError(
                    f"Binance HTTP {r.status_code}: {data.get('code')} - {data.get('msg')}"
                )
            else:
                print("BINANCE ERROR TEXT:", text)
                raise RuntimeError(f"Binance HTTP {r.status_code}: {text}")

        if data is None:
            return text

        self._maybe_raise_binance_error(data)
        return data

    async def _signed_get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Any:
        return await self._signed_request("GET", path, params)

    async def _signed_post(self, path: str, params: Optional[Dict[str, Any]] = None) -> Any:
        return await self._signed_request("POST", path, params)

    # =============================
    #        MARKET / INSTR
    # =============================
    async def _get_ticker(self, symbol: str) -> Dict[str, Any]:
        """
        Лучшие bid/ask по символу.
        GET /fapi/v1/ticker/bookTicker
        """
        sym = self.spot_like_symbol(symbol)
        data = await self._public_get("/fapi/v1/ticker/bookTicker", {"symbol": sym})
        return data

    async def _get_symbol_info(self, symbol: str) -> Dict[str, Any]:
        """
        Информация по инструменту из exchangeInfo.
        Кэшируем по символу.
        """
        sym = self.spot_like_symbol(symbol)
        if sym in self._symbol_info_cache:
            return self._symbol_info_cache[sym]

        data = await self._public_get("/fapi/v1/exchangeInfo", {"symbol": sym})
        symbols = data.get("symbols") or []
        if not symbols:
            raise RuntimeError(f"Symbol {sym} not found in exchangeInfo")
        info = symbols[0]
        self._symbol_info_cache[sym] = info
        return info

    async def _symbol_step_info(self, symbol: str) -> Tuple[Decimal, Decimal]:
        """
        Возвращает (min_qty, step) из фильтра LOT_SIZE.
        """
        info = await self._get_symbol_info(symbol)
        filters = info.get("filters") or []
        min_qty = _d("0")
        step = _d("0")
        for f in filters:
            if f.get("filterType") == "LOT_SIZE":
                min_qty = _d(f.get("minQty", "0"))
                step = _d(f.get("stepSize", "0"))
                break
        return min_qty, step
    async def _symbol_price_step(self, symbol: str) -> Decimal:
        """
        Возвращает шаг цены (tickSize) из фильтра PRICE_FILTER.
        """
        info = await self._get_symbol_info(symbol)
        filters = info.get("filters") or []
        for f in filters:
            if f.get("filterType") == "PRICE_FILTER":
                return _d(f.get("tickSize", "0"))
        return _d("0")


    async def usdt_to_qty(
        self,
        symbol: str,
        usdt_amount: float | str,
        *,
        side: Literal["buy", "sell"],
    ) -> str:
        """
        Конвертация USDT → qty по "адекватной" цене:
          1) bookTicker (bid/ask)
          2) markPrice
          3) last price
        Если все источники дают 0 или None — кидаем понятную ошибку.
        """
        sym = self.spot_like_symbol(symbol)

        # 1) Пытаемся взять bid/ask из bookTicker
        t = await self._get_ticker(sym)
        bid_dec = _d(t.get("bidPrice") or "0")
        ask_dec = _d(t.get("askPrice") or "0")

        price = _d("0")
        if side == "buy":
            # покупаем по ask, если он > 0, иначе fallback на bid
            if ask_dec > 0:
                price = ask_dec
            elif bid_dec > 0:
                price = bid_dec
        else:
            # продаём по bid, если он > 0, иначе fallback на ask
            if bid_dec > 0:
                price = bid_dec
            elif ask_dec > 0:
                price = ask_dec

        # 2) Если bookTicker не дал нормальную цену — пробуем markPrice
        mark_dec = _d("0")
        if price <= 0:
            mark_dec = await self._get_mark_price(sym)
            if mark_dec > 0:
                price = mark_dec

        # 3) Если всё ещё 0 — пробуем last price
        last_dec = _d("0")
        if price <= 0:
            last_dec = await self._get_last_price(sym)
            if last_dec > 0:
                price = last_dec

        # Если все источники мёртвые — валимся с понятной ошибкой
        if price <= 0:
            raise RuntimeError(
                f"Bad price for {sym}: "
                f"bid={bid_dec}, ask={ask_dec}, mark={mark_dec}, last={last_dec}"
            )

        # Дальше всё как раньше — считаем qty с учётом LOT_SIZE
        min_qty, step = await self._symbol_step_info(sym)

        qty = _d(usdt_amount) / price
        if step > 0:
            qty = _round_step(qty, step)
        if qty < min_qty:
            qty = min_qty

        return _trim_decimals(qty)


    # =============================
    #          LEVERAGE
    # =============================
    async def set_leverage(self, symbol: str, leverage: int | str) -> Dict[str, Any]:
        """
        Установить плечо для символа.
        POST /fapi/v1/leverage
        """
        sym = self.spot_like_symbol(symbol)
        params = {
            "symbol": sym,
            "leverage": int(leverage),
        }
        return await self._signed_post("/fapi/v1/leverage", params)


    async def _place_order(
        self,
        *,
        symbol: str,
        side: Literal["BUY", "SELL"],
        quantity: str,
        order_type: Literal["MARKET", "LIMIT"] = "MARKET",
        price: Optional[str] = None,
        client_oid: Optional[str] = None,
        reduce_only: Optional[bool] = None,
        position_side: Optional[Literal["BOTH", "LONG", "SHORT"]] = None,  # <= NEW
    ) -> Dict[str, Any]:
        sym = self.spot_like_symbol(symbol)
        params: Dict[str, Any] = {
            "symbol": sym,
            "side": side,
            "type": order_type,
            "quantity": quantity,
        }
        if position_side is not None:
            params["positionSide"] = position_side

        if order_type == "LIMIT":
            if not price:
                raise ValueError("price is required for LIMIT orders")
            params["price"] = price
            params["timeInForce"] = "GTC"

        if client_oid:
            params["newClientOrderId"] = client_oid

        if reduce_only is not None:
            params["reduceOnly"] = "true" if reduce_only else "false"

        return await self._signed_post("/fapi/v1/order", params)

    async def _close_side_fully(
        self,
        symbol: str,
        want_side: Literal["long", "short"],
        *,
        max_attempts: int = 6,
        sleep_after: float = 0.2
    ) -> List[Dict[str, Any]]:
        orders: List[Dict[str, Any]] = []

        for _ in range(max_attempts):
            size, pos = await self._get_side_size(symbol, want_side)
            if size <= 0:
                break

            # qty-фильтр именно для MARKET
            min_qty, step = await self._symbol_qty_step_info(symbol, market=True)

            qty = size
            if qty < min_qty:
                qty = min_qty

            # ключевой момент: округляем ВВЕРХ, чтобы не оставлять хвост
            if step > 0:
                qty = _round_step_up(qty, step)

            qty_str = _trim_decimals(qty)

            # hedge-mode поддержка
            ps = None
            if isinstance(pos, dict):
                ps_val = pos.get("positionSide")
                if ps_val in ("BOTH", "LONG", "SHORT"):
                    ps = ps_val

            order_side: Literal["BUY", "SELL"] = "SELL" if want_side == "long" else "BUY"

            order = await self._place_order(
                symbol=symbol,
                side=order_side,
                quantity=qty_str,
                order_type="MARKET",
                reduce_only=True,
                position_side=ps,
            )
            orders.append(order)

            await asyncio.sleep(sleep_after)

        return orders


    async def _create_stop_loss_for_position(
        self,
        symbol: str,
        *,
        side: Literal["long", "short"],
        client_oid: Optional[str] = None,
        offset_percent: Decimal = _d("0.01"),  # 1%
    ) -> Optional[Dict[str, Any]]:
        """
        Создаёт лимитный стоп-лосс по текущей позиции:
          - для long: стоп на ликвидацию + 1%
          - для short: стоп на ликвидацию - 1%

        Ордер ставится как LIMIT STOP (type=STOP) с reduceOnly=true.
        """
        positions = await self._single_position(symbol)
        if not positions:
            return None

        target = None
        for p in positions:
            try:
                amt = _d(p.get("positionAmt") or "0")
            except Exception:
                continue
            if side == "long" and amt > 0:
                target = p
                break
            if side == "short" and amt < 0:
                target = p
                break

        if target is None:
            return None

        # Ликвидационная цена
        try:
            liq = _d(target.get("liquidationPrice") or "0")
        except Exception:
            return None
        if liq <= 0:
            return None

        # Размер позиции
        try:
            amt_abs = _d(target.get("positionAmt") or "0").copy_abs()
        except Exception:
            return None
        if amt_abs <= 0:
            return None

        # Нормируем количество к LOT_SIZE
        min_qty, step = await self._symbol_step_info(symbol)
        qty_dec = amt_abs
        if step > 0:
            qty_dec = _round_step(qty_dec, step)
        if qty_dec < min_qty:
            qty_dec = min_qty
        qty_str = _trim_decimals(qty_dec)

        # Стоп-цена:
        #   long  -> ликвидация + 1%
        #   short -> ликвидация - 1%
        if side == "long":
            sl_price_dec = liq * (Decimal("1") + offset_percent)
        else:
            sl_price_dec = liq * (Decimal("1") - offset_percent)

        price_step = await self._symbol_price_step(symbol)
        if price_step > 0:
            sl_price_dec = _round_step(sl_price_dec, price_step)
        if sl_price_dec <= 0:
            return None

        stop_price_str = _trim_decimals(sl_price_dec)

        # Сторона ордера (закрываем позицию)
        order_side: Literal["BUY", "SELL"]
        if side == "long":
            order_side = "SELL"
        else:
            order_side = "BUY"

        sym = self.spot_like_symbol(symbol)
        params: Dict[str, Any] = {
            "symbol": sym,
            "side": order_side,
            "type": "STOP",  # лимитный стоп-ордер
            "quantity": qty_str,
            "price": stop_price_str,
            "stopPrice": stop_price_str,
            "timeInForce": "GTC",
            "reduceOnly": "true",
        }

        if client_oid:
            # отдельный clientOrderId для стоп-лосса
            params["newClientOrderId"] = (client_oid + "_SL")[:36]

        try:
            return await self._signed_post("/fapi/v1/order", params)
        except Exception as e:
            print(f"Не удалось создать стоп-лосс для {sym}: {e}")
            return None

    async def open_long(
        self,
        symbol: str,
        qty: str,
        *,
        order_type: Literal["MARKET", "LIMIT"] = "MARKET",
        price: Optional[str] = None,
        leverage: Optional[int | str] = None,
        client_oid: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Открыть/увеличить long (BUY).
        Для рыночного ордера дополнительно ставим лимитный стоп-лосс.
        """
        if leverage:
            await self.set_leverage(symbol, leverage)

        order = await self._place_order(
            symbol=symbol,
            side="BUY",
            quantity=qty,
            order_type=order_type,
            price=price,
            client_oid=client_oid,
            reduce_only=False,
        )

        # Только для MARKET – сразу навешиваем стоп-лосс
        # if order_type == "MARKET":
        #     try:
        #         # даём бирже чуть обновить positionRisk, чтобы появилась ликвидация
        #         await asyncio.sleep(0.1)
                
        #         await self._create_stop_loss_for_position(
        #             symbol,
        #             side="long",
        #             client_oid=client_oid,
        #         )
        #     except Exception as e:
        #         print(f"Ошибка при создании стоп-лосса для LONG {symbol}: {e}")

        return order

    async def open_short(
        self,
        symbol: str,
        qty: str,
        *,
        order_type: Literal["MARKET", "LIMIT"] = "MARKET",
        price: Optional[str] = None,
        leverage: Optional[int | str] = None,
        client_oid: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Открыть/увеличить short (SELL).
        Для рыночного ордера дополнительно ставим лимитный стоп-лосс.
        """
        if leverage:
            await self.set_leverage(symbol, leverage)

        order = await self._place_order(
            symbol=symbol,
            side="SELL",
            quantity=qty,
            order_type=order_type,
            price=price,
            client_oid=client_oid,
            reduce_only=False,
        )

        # if order_type == "MARKET":
        #     try:
        #         await asyncio.sleep(0.1)
        #         await self._create_stop_loss_for_position(
        #             symbol,
        #             side="short",
        #             client_oid=client_oid,
        #         )
        #     except Exception as e:
        #         print(f"Ошибка при создании стоп-лосса для SHORT {symbol}: {e}")

        return order


    # Удобные обёртки через USDT
    async def open_long_usdt(
        self,
        symbol: str,
        usdt_amount: float | str,
        *,
        order_type: Literal["MARKET", "LIMIT"] = "MARKET",
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
        order_type: Literal["MARKET", "LIMIT"] = "MARKET",
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
    #        POSITIONS RAW
    # =============================
    async def _all_positions_raw(self) -> List[Dict[str, Any]]:
        """
        Возвращает все позиции (positionRisk) с ненулевым объёмом.
        GET /fapi/v3/positionRisk
        """
        data = await self._signed_get("/fapi/v3/positionRisk", {})
        if not isinstance(data, list):
            return []
        out: List[Dict[str, Any]] = []
        for p in data:
            try:
                amt = float(p.get("positionAmt") or 0)
            except Exception:
                continue
            if abs(amt) > 0:
                out.append(p)
        return out

    async def _single_position(self, symbol: str) -> List[Dict[str, Any]]:
        """
        Список позиций по символу (может быть одна строка для BOTH или две для hedge LONG/SHORT).
        """
        sym = self.spot_like_symbol(symbol)
        data = await self._signed_get("/fapi/v3/positionRisk", {"symbol": sym})
        if not isinstance(data, list):
            return []
        out: List[Dict[str, Any]] = []
        for p in data:
            if p.get("symbol") != sym:
                continue
            try:
                amt = float(p.get("positionAmt") or 0)
            except Exception:
                continue
            if abs(amt) > 0:
                out.append(p)
        return out

    # =============================
    #        CLOSE FULL SIDE
    # =============================
    async def _get_side_size(self, symbol: str, want_side: Literal["long", "short"]) -> Tuple[Decimal, Optional[Dict[str, Any]]]:
        """
        Возвращает (size_abs, raw_position) для стороны long/short по symbol.
        Для one-way: positionAmt > 0 — long, < 0 — short.
        Для hedge: positionAmt > 0 или < 0 с учётом positionSide, но знак также отражает направление.
        """
        positions = await self._single_position(symbol)
        for p in positions:
            try:
                amt = _d(p.get("positionAmt") or "0")
            except Exception:
                continue
            if want_side == "long" and amt > 0:
                return amt.copy_abs(), p
            if want_side == "short" and amt < 0:
                return amt.copy_abs(), p
        return _d(0), None
    
    async def _get_order_realized_pnl(
        self,
        symbol: str,
        order: Optional[Dict[str, Any]],
        *,
        retries: int = 3,
        delay: float = 0.15,
    ) -> Optional[Decimal]:
        """
        Для переданного ордера (ответ /fapi/v1/order) подтягивает realizedPnl
        по его сделкам из /fapi/v1/userTrades и суммирует.

        Возвращает Decimal или None, если сделки не найдены.
        """
        if not order:
            return None

        order_id = order.get("orderId")
        if order_id is None:
            return None

        try:
            order_id_int = int(order_id)
        except Exception:
            return None

        sym = self.spot_like_symbol(order.get("symbol") or symbol)

        last_pnl: Optional[Decimal] = None

        for _ in range(retries):
            try:
                trades = await self._signed_get("/fapi/v1/userTrades", {
                    "symbol": sym,
                    "limit": 50,
                })
            except Exception:
                trades = None

            if isinstance(trades, list):
                pnl_sum = _d("0")
                found = False
                for tr in trades:
                    try:
                        if int(tr.get("orderId")) != order_id_int:
                            continue
                    except Exception:
                        continue
                    rp = tr.get("realizedPnl")
                    if rp is None:
                        continue
                    try:
                        pnl_sum += _d(rp)
                        found = True
                    except Exception:
                        continue

                if found:
                    last_pnl = pnl_sum
                    break

            # если не нашли сделки по этому ордеру — даём бирже время "догнать"
            await asyncio.sleep(delay)

        return last_pnl


    async def close_long_qty(self, symbol: str, size = None) -> Optional[Dict[str, Any]]:
        """
        Закрыть весь long по символу (если есть).
        В one-way режиме — SELL reduceOnly.
        """
        if not size:
            size, pos = await self._get_side_size(symbol, "long")
        if size <= 0:
            return None
        min_qty, step = await self._symbol_step_info(symbol)
        qty = size
        if step > 0:
            qty = _round_step(qty, step)
        if qty < min_qty:
            qty = min_qty
        return await self._place_order(
            symbol=symbol,
            side="SELL",
            quantity=_trim_decimals(qty),
            order_type="MARKET",
            reduce_only=True,
        )

    async def close_short_qty(self, symbol: str, size = None) -> Optional[Dict[str, Any]]:
        """
        Закрыть весь short по символу (если есть).
        В one-way режиме — BUY reduceOnly.
        """
        if not size:
            size, pos = await self._get_side_size(symbol, "short")
        if size <= 0:
            return None
        min_qty, step = await self._symbol_step_info(symbol)
        qty = size
        if step > 0:
            qty = _round_step(qty, step)
        if qty < min_qty:
            qty = min_qty
        return await self._place_order(
            symbol=symbol,
            side="BUY",
            quantity=_trim_decimals(qty),
            order_type="MARKET",
            reduce_only=True,
        )

    async def close_all_positions(self, symbol: str) -> Dict[str, Any]:
        long_orders = await self._close_side_fully(symbol, "long")
        long_pnl_dec = None
        if long_orders:
            total = _d("0")
            any_found = False
            for o in long_orders:
                p = await self._get_order_realized_pnl(symbol, o)
                if p is not None:
                    total += p
                    any_found = True
            long_pnl_dec = total if any_found else None

        short_orders = await self._close_side_fully(symbol, "short")
        short_pnl_dec = None
        if short_orders:
            total = _d("0")
            any_found = False
            for o in short_orders:
                p = await self._get_order_realized_pnl(symbol, o)
                if p is not None:
                    total += p
                    any_found = True
            short_pnl_dec = total if any_found else None

        total_pnl_dec = None
        if long_pnl_dec is not None or short_pnl_dec is not None:
            total_pnl_dec = _d("0")
            if long_pnl_dec is not None:
                total_pnl_dec += long_pnl_dec
            if short_pnl_dec is not None:
                total_pnl_dec += short_pnl_dec

        return {
            "long_closed": long_orders[-1] if long_orders else None,
            "short_closed": short_orders[-1] if short_orders else None,
            "long_realized_pnl": float(long_pnl_dec) if long_pnl_dec is not None else None,
            "short_realized_pnl": float(short_pnl_dec) if short_pnl_dec is not None else None,
            "total_realized_pnl": float(total_pnl_dec) if total_pnl_dec is not None else None,
        }

    # =============================
    #        OPEN POSITIONS (UI)
    # =============================
    async def get_open_positions(
        self,
        *,
        symbol: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """
        Унифицированный словарь по открытой позиции (как в BitgetAsyncClient.get_open_positions).

        Формат:
          {
            "opened_at": <ISO8601 or None>,
            "symbol": "BIOUSDT",
            "side": "long"|"short",
            "usdt": <float>,          # оценка маржи (notional / leverage)
            "leverage": <int>,
            "pnl": <str>,             # unrealized profit
            "entry_usdt": <float>,
            "entry_price": <str>,
            "market_price": <str>,
          }
        Если позиции нет — вернёт None.
        """
        if symbol is None:
            return None

        positions = await self._single_position(symbol)
        if not positions:
            return None

        # Берём первую непустую позицию
        p = positions[0]
        try:
            amt = float(p.get("positionAmt") or 0)
        except Exception:
            return None
        if abs(amt) == 0:
            return None

        side = "long" if amt > 0 else "short"
        leverage = float(p.get("leverage") or 0) or 1.0

        # notional может быть "0", поэтому пересчитаем при необходимости
        try:
            notional = abs(float(p.get("notional") or 0))
        except Exception:
            notional = 0.0
        if notional <= 0:
            try:
                mark_price = float(p.get("markPrice") or 0) or float(p.get("entryPrice") or 0)
                notional = abs(amt * mark_price)
            except Exception:
                notional = 0.0

        entry_usdt = notional / leverage if leverage > 0 else notional

        out: Dict[str, Any] = {
            "opened_at": _to_iso_ms(p.get("updateTime")),
            "symbol": self.spot_like_symbol(symbol),
            "side": side,
            "usdt": entry_usdt,
            "leverage": leverage,
            "entry_usdt": entry_usdt,
            "pnl": p.get("unRealizedProfit"),
            "entry_price": p.get("entryPrice"),
            "market_price": p.get("markPrice"),
            "liq_price": p.get("liquidationPrice")
        }
        return out

    # =============================
    #        BALANCE USDT
    # =============================
    async def get_usdt_balance(self) -> str:
        """
        Возвращает баланс фьючерсного кошелька в USDT.
        GET /fapi/v2/balance
        """
        data = await self._signed_get("/fapi/v2/balance", {})
        if not isinstance(data, list):
            return "0"
        total = _d("0")
        for row in data:
            if (row.get("asset") or "").upper() == "USDT":
                total += _d(row.get("balance") or "0")
        return _trim_decimals(total.normalize())


# ---- Пример использования ----
async def main():
    symbol = "BIOUSDT"
    async with BinanceAsyncFuturesClient(BINANCE_API_KEY, BINANCE_API_SECRET) as client:
        # Пример открытия позиции:
        # print(await client.open_long(symbol=symbol, qty="300", leverage=1))
        # print(await client.open_short(symbol=symbol, qty="120", leverage=1))

        # Пример открытия через USDT:
        # print(await client.open_long_usdt(symbol=symbol, usdt_amount=20, leverage=3))

        # Позиции
        # pos = await client.get_open_positions(symbol=symbol)
        # print("OPEN POSITION:", pos)

        # res = await client.close_all_positions(symbol="BIOUSDT")
        # print(res)

        res = await client.close_short_qty(symbol=symbol, size=300)

        # # Все позиции
        # all_pos = await client._all_positions_raw()
        # print("ALL POSITIONS:", all_pos)

        # # Баланс
        # bal = await client.get_usdt_balance()
        # print("USDT BALANCE:", bal)


if __name__ == "__main__":
    asyncio.run(main())
