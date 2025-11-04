# kucoin_async_futures_mainnet.py
from __future__ import annotations

import os
import hmac
import time
import base64
import json
import uuid
import hashlib
import asyncio
from typing import Optional, Literal, Dict, Any, List
from decimal import Decimal, ROUND_DOWN, ROUND_FLOOR
from datetime import datetime, timezone

import httpx
from dotenv import load_dotenv
load_dotenv()

KUCOIN_API_KEY        = os.getenv("KUCOIN_API_KEY", "")
KUCOIN_API_SECRET     = os.getenv("KUCOIN_API_SECRET", "")
KUCOIN_API_PASSPHRASE = os.getenv("KUCOIN_API_PASSPHRASE", "")


# ---------- numeric utils ----------
def _d(x) -> Decimal:
    return Decimal(str(x))

def _round_step(value: Decimal, step: Decimal) -> Decimal:
    if step == 0:
        return value
    q = (value / step).to_integral_value(rounding=ROUND_DOWN)
    return q * step


# ======================================================
#                KUCOIN Futures (USDT Perps)
# ======================================================
class KucoinAsyncFuturesClient:
    """
    Асинхронный клиент KuCoin Futures (USDT-margined perpetuals), без ccxt.

    Возможности:
    - синхрон времени с биржей (важно для подписи)
    - подписание приватных запросов (KC-API-* v2)
    - получение тикера и параметров контракта
    - перевод "хочу позицию на N USDT" -> "size контрактов"
    - открытие лонга / шорта
    - закрытие лонга / шорта
    - закрыть все позиции по символу
    - получить открытые позиции (c opened_at и PnL)
    - автофикс ошибки 330005:
        * переключить режим маржи (ISOLATED / CROSS)
        * если CROSS — установить плечо
        * повторить ордер
    """

    def __init__(
        self,
        api_key: str,
        api_secret: str,
        api_passphrase: str,
        *,
        base_url: str = "https://api-futures.kucoin.com",
        timeout: float = 15.0,
        default_leverage: int = 5,
        default_margin_mode: Literal["ISOLATED","CROSS"] = "ISOLATED",
    ):
        if not api_key or not api_secret or not api_passphrase:
            raise ValueError("KUCOIN_API_KEY / KUCOIN_API_SECRET / KUCOIN_API_PASSPHRASE не заданы")

        self.api_key        = api_key
        self.api_secret     = api_secret.encode("utf-8")
        self.api_passphrase = api_passphrase

        self.base_url       = base_url.rstrip("/")
        self._client        = httpx.AsyncClient(base_url=self.base_url, timeout=timeout)

        # настройки по умолчанию
        self._default_lev   = int(default_leverage)
        self._margin_mode   = default_margin_mode  # "ISOLATED" или "CROSS"

        # смещение времени в мс (server - local)
        self._time_offset_ms = 0

    # -------------- контекстный менеджер --------------
    async def __aenter__(self):
        await self._sync_time()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.close()

    async def close(self):
        await self._client.aclose()

    # ======================================================
    #                   ВРЕМЯ / ПОДПИСЬ
    # ======================================================

    async def _get_server_time_ms(self) -> int:
        """
        KuCoin Futures: GET /api/v1/timestamp
        Возвращает {"code":"200000","data":<ts_ms>}
        """
        r = await self._client.get("/api/v1/timestamp")
        r.raise_for_status()
        data = r.json()
        if data.get("code") != "200000":
            raise RuntimeError(f"KuCoin time error: {data}")
        server_ts = int(data["data"])
        return server_ts

    async def _sync_time(self):
        """
        Синхронизируем локальное время с серверным,
        чтобы KC-API-TIMESTAMP не выбивало 401.
        """
        local_before = int(time.time() * 1000)
        server = await self._get_server_time_ms()
        local_after = int(time.time() * 1000)
        # возьмем середину окна как "локальное представление запроса"
        local_mid = (local_before + local_after) // 2
        self._time_offset_ms = server - local_mid

    def _now_ms_str(self) -> str:
        """
        Возвращает timestamp в мс с поправкой на KuCoin-время
        """
        ts_ms = int(time.time() * 1000) + self._time_offset_ms
        return str(ts_ms)

    def _build_querystring(self, query: Optional[Dict[str, Any]]) -> str:
        """
        Стабильная строка query: сортируем ключи,
        чтобы подпись и фактический URL совпадали.
        """
        if not query:
            return ""
        items = [(k, "" if v is None else str(v)) for k,v in query.items()]
        items.sort(key=lambda kv: kv[0])
        return "&".join([f"{k}={v}" for k,v in items])

    def _dump_body_canonical(self, body: Optional[Dict[str, Any]]) -> str:
        """
        Стабильная сериализация тела:
        - без пробелов
        - в порядке добавления ключей в dict
        Мы будем передавать body в уже нужном порядке,
        подписывать именно этот JSON и отправлять именно этот JSON.
        """
        if not body:
            return ""
        return json.dumps(body, separators=(",", ":"), ensure_ascii=False)

    def _sign_headers(
        self,
        method: str,
        path: str,
        query: Optional[Dict[str, Any]],
        body: Optional[Dict[str, Any]],
        *,
        body_str: str,
        query_str: str,
    ) -> Dict[str, str]:
        """
        KuCoin Futures подпись (KC-API-KEY-VERSION: 2):

        prehash = timestamp + method + endpoint(+query) + body_str
        где endpoint(+query) = например "/api/v1/orders?symbol=BTCUSDTM"
        body_str = канонический json или "".

        Потом:
        KC-API-SIGN = base64(HMAC_SHA256(secret, prehash))
        KC-API-PASSPHRASE = base64(HMAC_SHA256(secret, passphrase))

        KC-API-TIMESTAMP = timestamp
        """
        ts = self._now_ms_str()

        endpoint = path
        if query_str:
            endpoint = endpoint + "?" + query_str

        prehash = f"{ts}{method.upper()}{endpoint}{body_str}"

        sign = base64.b64encode(
            hmac.new(self.api_secret, prehash.encode("utf-8"), hashlib.sha256).digest()
        ).decode()

        passphrase_sign = base64.b64encode(
            hmac.new(self.api_secret, self.api_passphrase.encode("utf-8"), hashlib.sha256).digest()
        ).decode()

        return {
            "KC-API-KEY": self.api_key,
            "KC-API-SIGN": sign,
            "KC-API-TIMESTAMP": ts,
            "KC-API-PASSPHRASE": passphrase_sign,
            "KC-API-KEY-VERSION": "2",
            "Content-Type": "application/json",
        }

    async def _public_get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Публичные ручки — без подписи,
        но мы всё равно сортируем query ради стабильности.
        """
        ordered_params: Dict[str, Any] = {}
        if params:
            for k,v in sorted(params.items(), key=lambda kv: kv[0]):
                ordered_params[k] = v

        r = await self._client.get(path, params=ordered_params)
        r.raise_for_status()
        data = r.json()
        if data.get("code") != "200000":
            raise RuntimeError(f"KuCoin public error: {data}")
        return data

    async def _private(
        self,
        method: Literal["GET","POST"],
        path: str,
        query: Optional[Dict[str, Any]] = None,
        body: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Универсальный приватный вызов.
        - Строим стабильный query_str
        - Строим стабильный body_str
        - Подписываем
        - Шлём именно тот же body_str в content=
        """
        # стабильно отсортированный query для подписи И для реального URL
        qstr = self._build_querystring(query)
        body_str = self._dump_body_canonical(body)

        headers = self._sign_headers(
            method, path, query, body,
            body_str=body_str,
            query_str=qstr,
        )

        # httpx: используем такой же отсортированный dict для params
        ordered_query: Dict[str, Any] = {}
        if query:
            for k,v in sorted(query.items(), key=lambda kv: kv[0]):
                ordered_query[k] = v

        if method == "GET":
            r = await self._client.get(
                path,
                params=ordered_query,
                headers=headers
            )
        else:
            r = await self._client.post(
                path,
                params=ordered_query,
                content=body_str.encode("utf-8"),
                headers=headers
            )

        r.raise_for_status()
        data = r.json()
        if data.get("code") != "200000":
            raise RuntimeError(f"KuCoin error: {data.get('code')} - {data.get('msg')} | {data}")
        return data

    # ======================================================
    #                   ИНФО О КОНТРАКТЕ / ЦЕНЕ
    # ======================================================

    @staticmethod
    def _normalize_symbol(symbol: str) -> str:
        """
        KuCoin Futures символы для USDT-перпов обычно заканчиваются на 'M':
        BTCUSDTM, ETHUSDTM, BIOUSDTM и т.д.

        Если нам дали 'BTCUSDT' — добавим 'M'.
        Если уже 'BTCUSDTM' — оставим.
        """
        s = symbol.upper().strip()
        if not s.endswith("M"):
            s = s + "M"
        return s

    async def _get_contract_info(self, symbol: str) -> Dict[str, Any]:
        """
        GET /api/v1/contracts/{symbol}

        Возвращает лот, multiplier, макс плечо и т.д.
        """
        sym = self._normalize_symbol(symbol)
        data = await self._public_get(f"/api/v1/contracts/{sym}")
        if "data" not in data or not data["data"]:
            raise RuntimeError("KuCoin contract_info error: empty data")
        return data["data"]

    async def _get_ticker(self, symbol: str) -> Dict[str, Any]:
        """
        FUTURES тикер:
        GET /api/v1/ticker?symbol=BTCUSDTM

        Ответ:
        {
          "code":"200000",
          "data":{
             "symbol":"BTCUSDTM",
             "sequence":"...",
             "side":"sell",
             "size":"291",
             "price":"0.2431",
             "bestBidPrice":"0.2436",
             "bestBidSize":"2326",
             "bestAskPrice":"0.244",
             "bestAskSize":"6843",
             "tradeId":"1696348345098",
             "ts": 1761511795018000000
          }
        }
        """
        sym = self._normalize_symbol(symbol)
        data = await self._public_get("/api/v1/ticker", params={"symbol": sym})
        if "data" not in data or not data["data"]:
            raise RuntimeError("KuCoin ticker error: empty data")
        return data["data"]

    # ======================================================
    #           Конвертация USDT -> size контрактов
    # ======================================================
    async def usdt_to_size(
        self,
        symbol: str,
        usdt_amount: float | str,
        *,
        side: Literal["buy","sell"],  # если мы покупаем → считаем по ask, продаём → по bid
    ) -> str:
        info = await self._get_contract_info(symbol)
        tick = await self._get_ticker(symbol)

        best_ask = _d(tick.get("bestAskPrice", "0"))
        best_bid = _d(tick.get("bestBidPrice", "0"))

        px = best_ask if side == "buy" else best_bid
        if px <= 0:
            px = _d(tick.get("price", "0"))  # fallback = last traded price
        if px <= 0:
            raise RuntimeError(f"Bad price for {symbol}: {px}")

        # multiplier — сколько базового актива или условной нотации в 1 контракте
        mul = _d(info.get("multiplier", "1"))
        if mul <= 0:
            mul = _d("1")

        usdt = _d(usdt_amount)

        # стоимость одного контракта (примерно в USDT)
        cost_per_contract = px * mul
        if cost_per_contract <= 0:
            raise RuntimeError("cost_per_contract is zero")

        # целое число контрактов по деньгам
        contracts = usdt / cost_per_contract
        size_int = int(contracts.to_integral_value(rounding=ROUND_FLOOR))

        # lotSize — минимальный шаг size
        lot = _d(info.get("lotSize", "1"))
        if lot <= 0:
            lot = _d("1")

        size_adj = (_d(size_int) / lot).to_integral_value(rounding=ROUND_FLOOR) * lot
        if size_adj <= 0:
            size_adj = lot

        return str(int(size_adj))

    # ======================================================
    #       Управление маржой и плечом (для ретрая 330005)
    # ======================================================

    async def _switch_margin_mode(
        self,
        symbol: str,
        margin_mode: Literal["ISOLATED","CROSS"],
    ) -> Dict[str, Any]:
        """
        POST /api/v2/position/changeMarginMode
        body {
          "symbol": "BTCUSDTM",
          "marginMode": "ISOLATED" | "CROSS"
        }

        Переключает режим маржи. Нужно для ошибки 330005.
        """
        sym = self._normalize_symbol(symbol)
        body = {
            "symbol": sym,
            "marginMode": margin_mode,
        }
        return await self._private(
            "POST",
            "/api/v2/position/changeMarginMode",
            body=body
        )

    async def _set_cross_leverage(
        self,
        symbol: str,
        leverage: str | int,
    ) -> Dict[str, Any]:
        """
        POST /api/v2/changeCrossUserLeverage
        body {
          "symbol": "BTCUSDTM",
          "leverage": "10"
        }

        Работает только для CROSS. Для ISOLATED KuCoin обычно принимает плечо прямо в ордере.
        """
        sym = self._normalize_symbol(symbol)
        body = {
            "symbol": sym,
            "leverage": str(leverage),
        }
        return await self._private(
            "POST",
            "/api/v2/changeCrossUserLeverage",
            body=body
        )

    # ======================================================
    #                 Размещение ордеров
    # ======================================================
    async def _place_order(
        self,
        *,
        symbol: str,
        side: Literal["buy","sell"],
        trade_type: Literal["OPEN","CLOSE"],  # OPEN для открытия, CLOSE для закрытия
        size: str,
        order_type: Literal["market","limit"] = "market",
        price: Optional[str] = None,
        leverage: Optional[int | str] = None,
        client_oid: Optional[str] = None,
        _retry_on_330005: bool = True,
    ) -> Dict[str, Any]:
        """
        KuCoin Futures /api/v1/orders ожидает:
          clientOid   (обязательно)
          side        "buy"/"sell"
          symbol      "BTCUSDTM"
          type        "market"/"limit"
          size        "<кол-во контрактов>"
          tradeType   "OPEN" / "CLOSE"
          leverage    "5" (строка)

        limit ордера требуют price.
        """
        sym = self._normalize_symbol(symbol)
        oid = client_oid or str(uuid.uuid4())

        lev_to_use = str(leverage if leverage is not None else self._default_lev)

        # тело задаём в фиксированном порядке ключей.
        body: Dict[str, Any] = {
            "clientOid": oid,
            "side": side,
            "symbol": sym,
            "type": order_type,
            "size": str(size),
            "tradeType": trade_type,
            "leverage": lev_to_use,
        }
        if order_type == "limit":
            if not price:
                raise ValueError("price is required for limit order")
            body["price"] = str(price)

        try:
            data = await self._private("POST", "/api/v1/orders", body=body)
            return data

        except RuntimeError as e:
            msg = str(e)

            # самый частый кейс KuCoin futures:
            # 330005 - "The order's margin mode does not match the selected one. Please switch and try again."
            need_retry = ("330005" in msg)

            if _retry_on_330005 and need_retry:
                # 1. Ставим нужный режим маржи (ISOLATED / CROSS),
                #    соответствующий self._margin_mode
                await self._switch_margin_mode(symbol, self._margin_mode)

                # 2. Если режим CROSS, KuCoin просит отдельно выставить плечо
                #    через changeCrossUserLeverage.
                if self._margin_mode == "CROSS":
                    await self._set_cross_leverage(symbol, lev_to_use)

                # 3. Повторяем ордер ровно один раз
                data2 = await self._private("POST", "/api/v1/orders", body=body)
                return data2

            # если не 330005 или повтор уже делали — бросаем выше
            raise

    # ======================================================
    #           High-level открытие позиций
    # ======================================================
    async def open_long_usdt(
        self,
        symbol: str,
        usdt_amount: float | str,
        *,
        order_type: Literal["market","limit"] = "market",
        price: Optional[str] = None,
        leverage: Optional[int | str] = None,
        client_oid: Optional[str] = None,
    ):
        """
        Открыть лонг:
        side="buy", tradeType="OPEN"
        """
        size = await self.usdt_to_size(symbol, usdt_amount, side="buy")
        return await self._place_order(
            symbol=symbol,
            side="buy",
            trade_type="OPEN",
            size=size,
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
        order_type: Literal["market","limit"] = "market",
        price: Optional[str] = None,
        leverage: Optional[int | str] = None,
        client_oid: Optional[str] = None,
    ):
        """
        Открыть шорт:
        side="sell", tradeType="OPEN"
        """
        size = await self.usdt_to_size(symbol, usdt_amount, side="sell")
        return await self._place_order(
            symbol=symbol,
            side="sell",
            trade_type="OPEN",
            size=size,
            order_type=order_type,
            price=price,
            leverage=leverage,
            client_oid=client_oid,
        )

    # ======================================================
    #           High-level закрытие позиций
    # ======================================================
    async def _get_positions_raw(self, symbol: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Простое чтение /api/v1/positions (приватный GET).
        Возвращаем список позиций как есть (list[dict]).
        """
        query: Dict[str, Any] = {}
        if symbol:
            query["symbol"] = self._normalize_symbol(symbol)

        data = await self._private("GET", "/api/v1/positions", query=query)
        items = data.get("data") or []
        return items

    async def _close_position_full(
        self,
        *,
        symbol: str,
        margin_mode: str,
        position_side: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Закрыть позицию целиком через closeOrder=true.

        Требования KuCoin Futures:
        - POST /api/v1/orders
        - Обязательные поля:
            clientOid
            symbol
            marginMode        ("ISOLATED" или "CROSS" — должен совпадать с позицией)
            closeOrder        = true  (сигнал: закрыть позицию)
            type              = "market"  <-- важно, иначе биржа считает, что это лимит и требует price
        - В hedge-режиме нужно явно указать positionSide ("LONG"/"SHORT").
          В one-way режиме приходит "BOTH", и это тоже ок.

        Нам НЕ нужно указывать:
        - side
        - size
        - leverage
        - price
        Биржа сама определит сторону (sell для LONG, buy для SHORT)
        и объём (всю позицию).
        """
        sym = self._normalize_symbol(symbol)
        oid = str(uuid.uuid4())

        # порядок ключей в body важен (для подписи), поэтому сразу кладём в итоговом порядке
        body: Dict[str, Any] = {
            "clientOid": oid,
            "symbol": sym,
            "marginMode": margin_mode,   # "ISOLATED" или "CROSS" как на позиции
            "closeOrder": True,          # магический флаг "закрой позицию полностью"
            "type": "market",            # <---- КРИТИЧНО: теперь это явно маркет, без price
        }

        # если биржа вернула конкретный positionSide (LONG/SHORT/BOTH), пробрасываем
        if position_side:
            body["positionSide"] = position_side

        data = await self._private("POST", "/api/v1/orders", body=body)
        return data

    async def close_all_positions(self, symbol: str) -> Dict[str, Any]:
        """
        Новая версия close_all_positions:
        1. Берём список позиций по символу
        2. Для каждой позиции, где currentQty != 0, шлём closeOrder:true
        3. Возвращаем результат по LONG и SHORT отдельно
        """

        sym_norm = self._normalize_symbol(symbol)
        positions = await self._get_positions_raw(sym_norm)

        # KuCoin может вернуть 1 позицию (one-way, positionSide=BOTH)
        # или 2 позиции (hedge mode: одна LONG с currentQty>0, одна SHORT с currentQty<0)
        long_pos = None
        short_pos = None

        for p in positions:
            if p.get("symbol", "").upper() != sym_norm:
                continue

            qty = _d(p.get("currentQty", "0"))
            if qty == 0:
                continue

            # Разбираем направление
            side = p.get("positionSide")  # "LONG" / "SHORT" / "BOTH"
            margin_mode = p.get("marginMode", self._margin_mode)

            if qty > 0:
                # лонг
                long_pos = {
                    "qty": qty,
                    "positionSide": side,
                    "marginMode": margin_mode,
                }
            elif qty < 0:
                # шорт
                short_pos = {
                    "qty": qty,
                    "positionSide": side,
                    "marginMode": margin_mode,
                }

        result: Dict[str, Any] = {}

        # --- закрываем LONG (если есть)
        if long_pos:
            try:
                result["long_closed"] = await self._close_position_full(
                    symbol=sym_norm,
                    margin_mode=long_pos["marginMode"],
                    position_side=long_pos["positionSide"],
                )
            except RuntimeError as e:
                result["long_closed"] = {
                    "error": "close_long_failed",
                    "message": str(e),
                    "positionSide": long_pos["positionSide"],
                    "marginMode": long_pos["marginMode"],
                    "qty": str(long_pos["qty"]),
                }
        else:
            result["long_closed"] = None

        # --- закрываем SHORT (если есть)
        if short_pos:
            try:
                result["short_closed"] = await self._close_position_full(
                    symbol=sym_norm,
                    margin_mode=short_pos["marginMode"],
                    position_side=short_pos["positionSide"],
                )
            except RuntimeError as e:
                result["short_closed"] = {
                    "error": "close_short_failed",
                    "message": str(e),
                    "positionSide": short_pos["positionSide"],
                    "marginMode": short_pos["marginMode"],
                    "qty": str(short_pos["qty"]),
                }
        else:
            result["short_closed"] = None

        return result

    async def close_long_usdt(
        self,
        symbol: str,
        usdt_amount: float | str,  # не используем, закрываем весь доступный лонг
        *,
        order_type: Literal["market","limit"] = "market",
        price: Optional[str] = None,
        client_oid: Optional[str] = None,
    ):
        """
        Закрыть лонг:
        лонг = позиция >0
        чтобы закрыть лонг, надо ПРОДАТЬ с tradeType="CLOSE":
        side="sell", tradeType="CLOSE"
        """
        size_avail = await self._get_available_size_to_close(symbol, "long")
        if not size_avail:
            return None

        try:
            return await self._place_order(
                symbol=symbol,
                side="sell",
                trade_type="CLOSE",
                size=str(size_avail),
                order_type=order_type,
                price=price,
                leverage=None,      # на закрытие KuCoin обычно не требует плечо
                client_oid=client_oid,
            )
        except RuntimeError as e:
            return {
                "error": "close_long_failed",
                "message": str(e),
                "requested_size": size_avail,
            }

    async def close_short_usdt(
        self,
        symbol: str,
        usdt_amount: float | str,  # не используем, закрываем весь доступный шорт
        *,
        order_type: Literal["market","limit"] = "market",
        price: Optional[str] = None,
        client_oid: Optional[str] = None,
    ):
        """
        Закрыть шорт:
        шорт = позиция <0
        чтобы закрыть шорт, надо КУПИТЬ с tradeType="CLOSE":
        side="buy", tradeType="CLOSE"
        """
        size_avail = await self._get_available_size_to_close(symbol, "short")
        if not size_avail:
            return None

        try:
            return await self._place_order(
                symbol=symbol,
                side="buy",
                trade_type="CLOSE",
                size=str(size_avail),
                order_type=order_type,
                price=price,
                leverage=None,
                client_oid=client_oid,
            )
        except RuntimeError as e:
            return {
                "error": "close_short_failed",
                "message": str(e),
                "requested_size": size_avail,
            }



    # ======================================================
    #                 Чтение открытых позиций
    # ======================================================
    @staticmethod
    def _iso_from_ms(ms: int | str | None) -> Optional[str]:
        """
        KuCoin обычно кладёт timestamps в мс.
        Иногда могут быть секунды — подстрахуемся.
        """
        if ms is None:
            return None
        try:
            val = int(ms)
        except Exception:
            return None
        if val < 10**12:
            # если вдруг секунды
            val = val * 1000
        try:
            return datetime.fromtimestamp(val / 1000, tz=timezone.utc).isoformat()
        except Exception:
            return None

    async def get_open_positions(
        self,
        symbol: Optional[str] = None
    ) -> Optional[List[Dict[str, Any]]]:
        """
        Возвращает список открытых позиций в удобном виде:
        [
          {
            "opened_at": "...ISO8601...",
            "symbol": "BIOUSDTM",
            "type": "long" | "short",
            "size": "109",
            "entry_price": "0.09105239",
            "mark_price": "0.09109",
            "pnl_unrealized": "0.0041",
            "leverage": "5",
            "value_usdt": "9.92471"
          },
          ...
        ]
        """
        query: Dict[str, Any] = {}
        if symbol:
            query["symbol"] = self._normalize_symbol(symbol)

        data = await self._private("GET", "/api/v1/positions", query=query)
        pos_list = data.get("data") or []
        if not pos_list:
            return None

        out: List[Dict[str, Any]] = []
        for p in pos_list:
            qty_now = _d(p.get("currentQty", "0"))
            if qty_now == 0:
                continue

            pos_side = "long" if qty_now > 0 else "short"

            mark_price  = _d(p.get("markPrice", "0"))
            lev         = str(p.get("leverage", ""))

            # оценка текущей "стоимости" позиции в USDT:
            # |qty| * mark_price * multiplier
            mul = _d(p.get("multiplier", "1"))
            if mul <= 0:
                mul = _d("1")
            position_value = abs(qty_now) * mark_price * mul

            opened_raw = (
                p.get("openTime") or
                p.get("updatedAt") or
                p.get("createdAt")
            )
            opened_iso = self._iso_from_ms(opened_raw)

            out.append({
                "opened_at": opened_iso,
                "symbol": p.get("symbol", ""),
                "side": pos_side,
                "usdt": format(position_value, "f"),
                "leverage": lev,
                "pnl": str(p.get("unrealisedPnl", "0"))
            })

        return out or None

    async def get_usdt_balance(self) -> str:
        """
        Вернёт ТОЛЬКО доступный торговый баланс в USDT (available) как строку.
        Источник: GET /api/v1/account-overview?currency=USDT

        Поля у KuCoin Futures могут называться по-разному у разных аккаунтов:
        - "availableBalance" (основное)
        - "availableMargin" / "available" (варианты)
        - фолбэк на "cashBalance" - удерживая смысл "свободно"
        """
        # 1) основной эндпоинт с явной валютой
        data = await self._private("GET", "/api/v1/account-overview", query={"currency": "USDT"})
        info = data.get("data") or {}

        def _pick(*keys: str) -> Decimal:
            for k in keys:
                v = info.get(k)
                if v is not None:
                    try:
                        return _d(v)
                    except Exception:
                        continue
            return _d(0)

        # пытаемся вытащить именно доступные средства
        available = _pick("availableBalance", "availableMargin", "available", "cashBalance")
        if available > 0:
            return format(available, "f")

        # 2) редкий фолбэк: без currency (на всякий случай)
        data2 = await self._private("GET", "/api/v1/account-overview")
        info2 = data2.get("data") or {}
        available2 = _d(str(
            info2.get("availableBalance")
            or info2.get("availableMargin")
            or info2.get("available")
            or info2.get("cashBalance")
            or "0"
        ))
        return format(available2, "f")


# ---- простая отладка ----
async def _example():
    sym = "BIOUSDT"  # клиент сам превратит в "BIOUSDTM"

    async with KucoinAsyncFuturesClient(
        KUCOIN_API_KEY,
        KUCOIN_API_SECRET,
        KUCOIN_API_PASSPHRASE,
        default_leverage=5,
        default_margin_mode="ISOLATED",  # или "CROSS"
    ) as kc:

        # # открыть лонг на ~10 USDT:
        # print("OPEN LONG:", await kc.open_long_usdt(sym, 10, leverage=5))

        # # открыть лонг на ~10 USDT:
        # print("OPEN LONG:", await kc.open_short_usdt(sym, 10, leverage=5))

        # # посмотреть открытые позиции
        # print("OPEN POS:", await kc.get_open_positions(sym))

        # # попробовать закрыть обе стороны
        # print("CLOSE ALL:", await kc.close_all_positions(sym))
        print(float(await kc.get_usdt_balance()))


if __name__ == "__main__":
    asyncio.run(_example())
