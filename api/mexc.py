from __future__ import annotations

import os
import hmac
import time
import json
import hashlib
import asyncio
import urllib.parse
from typing import Optional, Literal, Dict, Any, List, Tuple
from decimal import Decimal, ROUND_DOWN
from datetime import datetime, timezone
import ssl
import httpx
from dotenv import load_dotenv
load_dotenv()

SSL_CTX = ssl.create_default_context()
MEXC_API_KEY    = os.getenv("MEXC_API_KEY", "")
MEXC_API_SECRET = os.getenv("MEXC_API_SECRET", "")

# ---------------------------------
# utils: decimals, time, formatting
# ---------------------------------
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


class MEXCAsyncFuturesClient:
    """
    Асинхронный клиент для USDT-перпетуалов MEXC (contract.mexc.com).

    Ключевые моменты по MEXC Futures API (актуально на 27 окт 2025, см. оф. доку). :contentReference[oaicite:0]{index=0}
    - Базовый URL приватки: https://contract.mexc.com
    - Подпись:
        requestParamString = (GET/DELETE -> отсортированный query "k=v&...",
                              POST -> сырой JSON body)
        sign_input = apiKey + reqTime + requestParamString
        signature  = hex(HMAC_SHA256(secret, sign_input))
      И это кладём в заголовок `Signature`.
      В заголовках также `ApiKey`, `Request-Time` и опц. `Recv-Window`. :contentReference[oaicite:1]{index=1}
    - Ордеры:
        POST /api/v1/private/order/submit
        поля:
            symbol      "BTC_USDT"
            price       decimal (для рынка можно не слать, биржа возьмёт маркет)
            vol         decimal (объём контрактов)
            side        int (1 open long, 2 close short, 3 open short, 4 close long)
            type        int (1 лимит, 5 маркет)
            openType    int (1 isolated, 2 cross)
            leverage    int (нужно для isolated)
            externalOid string (клиентский id, опц.)
            positionId  long  (лучше передавать при закрытии, опц.)
            ...
      :contentReference[oaicite:2]{index=2}
    - Позиции:
        GET /api/v1/private/position/open_positions[?symbol=...]
      Возвращает список с полями positionType (1 long / 2 short), holdVol, holdAvgPrice, leverage, createTime ... :contentReference[oaicite:3]{index=3}
    - Плечо:
        POST /api/v1/private/position/change_leverage
        Если позиции ещё нет, надо передать {openType, leverage, symbol, positionType}. :contentReference[oaicite:4]{index=4}
    """

    def __init__(
        self,
        api_key: str,
        api_secret: str,
        *,
        timeout: float = 15.0,
        base_url: str = "https://contract.mexc.com",
        default_margin_mode: Literal["isolated","cross"] = "isolated",
        default_leverage: int | str = 5,
    ):
        if not api_key or not api_secret:
            raise ValueError("MEXC_API_KEY / MEXC_API_SECRET не заданы")

        self.api_key    = api_key
        self.api_secret = api_secret.encode("utf-8")
        self.base_url   = base_url.rstrip("/")
        self.timeout    = timeout

        # MEXC ожидает openType в виде int: 1=isolated, 2=cross
        self.margin_mode = default_margin_mode
        self.open_type_num = 1 if default_margin_mode == "isolated" else 2

        self.default_leverage = str(default_leverage)

        self._client    = httpx.AsyncClient(base_url=self.base_url, timeout=timeout, verify=SSL_CTX)

    # --- context manager ---
    async def __aenter__(self):
        return self
    async def __aexit__(self, exc_type, exc, tb):
        await self.close()
    async def close(self):
        await self._client.aclose()

    # ---------------------------------
    # symbol helpers
    # ---------------------------------
    @staticmethod
    def spot_like_symbol_to_mexc(symbol: str) -> str:
        """
        'BIOUSDT' -> 'BIO_USDT'
        Простое правило: <BASE>USDT -> <BASE>_USDT.
        Если уже вида XXX_YYY - оставляем.
        """
        s = symbol.upper()
        if "_" in s:
            return s
        if s.endswith("USDT"):
            base = s[:-4]
            return f"{base}_USDT"
        return s

    # ---------------------------------
    # signing helpers
    # ---------------------------------
    @staticmethod
    def _encode_params_for_sign(params: Dict[str, Any]) -> Tuple[str, str]:
        """
        Возвращает (requestParamString_for_sign, query_string_for_url)

        По доке:
        - GET/DELETE: нужно отсортировать по ключу, URL-энкодить значения, собрать "k=v&k2=v2".
        - В URL мы тоже кладём ?k=...&k2=...
        - Если значение None -> вообще не включаем этот ключ.
        - Если после фильтрации нет полей -> обе строки пустые.
        """
        if not params:
            return "", ""

        # фильтруем None
        filtered = {k: v for k, v in params.items() if v is not None}

        if not filtered:
            return "", ""

        # сортируем ключи
        items = sorted(filtered.items(), key=lambda kv: kv[0])

        parts = []
        for k, v in items:
            val_str = "" if v is None else str(v)
            # MEXC требует urlEncode(value). Они хотят ' ' -> '%20', не '+'
            enc_v = urllib.parse.quote(val_str, safe='')
            parts.append(f"{k}={enc_v}")

        param_str = "&".join(parts)
        query_str = "?" + param_str if param_str else ""

        return param_str, query_str

    def _sign(self, req_time: str, request_param_str: str) -> str:
        """
        sign_input = apiKey + reqTime + request_param_str
        signature = hex(HMAC_SHA256(secret, sign_input))
        """
        base = f"{self.api_key}{req_time}{request_param_str}"
        digest = hmac.new(self.api_secret, base.encode("utf-8"), hashlib.sha256).hexdigest()
        return digest

    def _auth_headers(self, req_time: str, signature: str) -> Dict[str, str]:
        return {
            "Content-Type": "application/json",
            "ApiKey": self.api_key,
            "Request-Time": req_time,
            "Signature": signature,
            "Recv-Window": "5000",
        }

    @staticmethod
    def _err(data: Dict[str, Any]) -> str:
        # типичный приватный ответ при ошибке:
        # {"success":False,"code":602,"message":"签名验证失败!"}
        return f"{data.get('code')} - {data.get('message')}"

    # ---------------------------------
    # low-level HTTP: public (без подписи)
    # ---------------------------------
    async def _get_public(self, path: str, params: Dict[str, Any]) -> Dict[str, Any]:
        # это для тикера/контракта и т.п. (public endpoints не требуют подпись)
        req_param_str, query_str = self._encode_params_for_sign(params)
        # мы всё равно можем использовать encode_params_for_sign чтобы не дублировать код
        url = f"{path}{query_str}"

        r = await self._client.get(url)
        r.raise_for_status()
        data = r.json()
        return data

    # ---------------------------------
    # low-level HTTP: private (нужна подпись)
    # ---------------------------------
    async def _get_private(self, path: str, params: Dict[str, Any]) -> Dict[str, Any]:
        req_time = _now_ms()
        req_param_str, query_str = self._encode_params_for_sign(params)
        signature = self._sign(req_time, req_param_str)
        headers = self._auth_headers(req_time, signature)

        url = f"{path}{query_str}"
        r = await self._client.get(url, headers=headers)
        r.raise_for_status()
        data = r.json()
        if data.get("success") is False:
            raise RuntimeError(f"MEXC error: {self._err(data)} | {data}")
        return data

    async def _post_private(self, path: str, body: Dict[str, Any]) -> Dict[str, Any]:
        req_time = _now_ms()
        # тело должно быть JSON строкой (camelCase ключи мы уже даём вручную)
        body_json = json.dumps(body, separators=(",", ":"), ensure_ascii=False)

        signature = self._sign(req_time, body_json)
        headers = self._auth_headers(req_time, signature)

        r = await self._client.post(path, headers=headers, content=body_json)
        r.raise_for_status()
        data = r.json()
        if data.get("success") is False:
            raise RuntimeError(f"MEXC error: {self._err(data)} | {data}")
        return data

    # ---------------------------------
    # market / instrument info (public)
    # ---------------------------------
    async def _get_ticker(self, symbol_mexc: str) -> Dict[str, Any]:
        # GET /api/v1/contract/ticker?symbol=BTC_USDT (public) :contentReference[oaicite:5]{index=5}
        data = await self._get_public("/api/v1/contract/ticker", {"symbol": symbol_mexc})
        # формат: {"success":true,"code":0,"data":{...}}
        if isinstance(data, dict) and "data" in data:
            return data["data"] or {}
        return data

    async def _get_contract_detail(self, symbol_mexc: str) -> Dict[str, Any]:
        # GET /api/v1/contract/detail?symbol=BTC_USDT (public) :contentReference[oaicite:6]{index=6}
        data = await self._get_public("/api/v1/contract/detail", {"symbol": symbol_mexc})
        # формат: {"success":true,"code":0,"data":[{...}]}
        arr = data.get("data") if isinstance(data, dict) else None
        if isinstance(arr, list) and arr:
            return arr[0]
        if isinstance(arr, dict):
            return arr
        raise RuntimeError(f"Contract detail not found for {symbol_mexc} | {data}")

    async def usdt_to_qty(
        self,
        symbol: str,
        usdt_amount: float | str,
        *,
        side: Literal["buy","sell"],
    ) -> str:
        """
        Конвертация USDT -> объём контракта (vol) через лучшую цену.
        side="buy": берём ask1, side="sell": bid1.
        """
        sym = self.spot_like_symbol_to_mexc(symbol)
        t = await self._get_ticker(sym)

        # тикер выглядит типа:
        # { "symbol":"BTC_USDT","lastPrice":"...","bid1":"...","ask1":"..." , ... }
        px_str = (t.get("ask1") if side == "buy" else t.get("bid1")) or t.get("lastPrice")
        price = _d(px_str or "0")
        if price <= 0:
            raise RuntimeError(f"Bad price for {sym}: {price}")

        info = await self._get_contract_detail(sym)
        # контрактная инфа даёт minVol и volScale (или volPrecision) :contentReference[oaicite:7]{index=7}
        min_vol = _d(info.get("minVol", "0"))

        # volScale = кол-во знаков после запятой в объёме. Иногда это volScale, иногда volPrecision
        vol_scale = info.get("volScale", info.get("volPrecision", 0))
        vol_scale = int(vol_scale or 0)

        step = _d("1") / (_d(10) ** vol_scale) if vol_scale > 0 else _d("0")

        qty = _d(usdt_amount) / price
        if step > 0:
            qty = _round_step(qty, step)
        if qty < min_vol:
            qty = min_vol
        return _trim_decimals(qty)

    # ---------------------------------
    # leverage helpers
    # ---------------------------------
    async def set_leverage_both(
        self,
        symbol: str,
        leverage: int | str,
    ) -> Dict[str, Any]:
        """
        На MEXC плечо настраивается через POST /api/v1/private/position/change_leverage. :contentReference[oaicite:8]{index=8}

        Вариант "у меня ещё нет позиции":
            {
              "openType": 1,           # 1 isolated / 2 cross
              "leverage": 20,
              "symbol": "BTC_USDT",
              "positionType": 1        # 1 long 2 short
            }

        Чтобы покрыть и long, и short в hedge-режиме — вызываем два раза.
        Если аккаунт в one-way режиме, вторая попытка с positionType=2 обычно просто вернёт ту же инфу или не нужна.
        Мы не знаем режим заранее, но двойной вызов безопасен.
        """
        sym = self.spot_like_symbol_to_mexc(symbol)
        body_long = {
            "openType": self.open_type_num,
            "leverage": int(leverage),
            "symbol": sym,
            "positionType": 1,  # long
        }
        body_short = {
            "openType": self.open_type_num,
            "leverage": int(leverage),
            "symbol": sym,
            "positionType": 2,  # short
        }

        res_long  = await self._post_private("/api/v1/private/position/change_leverage", body_long)
        res_short = await self._post_private("/api/v1/private/position/change_leverage", body_short)

        return {"long": res_long, "short": res_short}

    # ---------------------------------
    # order submit helper
    # ---------------------------------
    async def _submit_order(
        self,
        *,
        symbol_mexc: str,
        side_code: int,  # 1=open long,2=close short,3=open short,4=close long
        vol: str,
        order_type: Literal["market","limit"] = "market",
        price: Optional[str] = None,
        leverage: Optional[int | str] = None,
        client_order_id: Optional[str] = None,
        position_id: Optional[int | str] = None,
    ) -> Dict[str, Any]:
        """
        POST /api/v1/private/order/submit  (подпись обязательна) :contentReference[oaicite:9]{index=9}

        MEXC поля:
          symbol      str
          price       decimal (для market можно опустить)
          vol         decimal
          leverage    int (если isolated обязательно)
          side        int (1 open long, 2 close short, 3 open short, 4 close long)
          type        int (1 лимит, 5 маркет)
          openType    int (1 isolated, 2 cross)
          positionId  long (опц., особенно для закрытия)
          externalOid str (клиентский id)
          positionMode int (опц., 1 hedge, 2 one-way)
          reduceOnly   bool (опц., для one-way)
        """

        type_num = 5 if order_type == "market" else 1

        body: Dict[str, Any] = {
            "symbol": symbol_mexc,
            "vol": vol,
            "side": side_code,
            "type": type_num,
            "openType": self.open_type_num,
        }

        # leverage: биржа требует на изолированном плече при открытии
        lev_to_send = leverage if leverage is not None else self.default_leverage
        if lev_to_send is not None:
            body["leverage"] = int(lev_to_send)

        # limit price (для лимита обязательно; для маркета обычно не нужно)
        if order_type == "limit":
            if not price:
                raise RuntimeError("limit order requires price")
            body["price"] = price
        else:
            if price:
                # если кто-то всё-таки передал price для market — просто прокинем
                body["price"] = price

        if client_order_id:
            body["externalOid"] = client_order_id
        if position_id:
            body["positionId"] = int(position_id)

        data = await self._post_private("/api/v1/private/order/submit", body)
        return data

    # ---------------------------------
    # high-level open funcs (совместимы с Bitget стилем)
    # ---------------------------------
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
        sym = self.spot_like_symbol_to_mexc(symbol)
        # side_code: 1 = open long
        return await self._submit_order(
            symbol_mexc=sym,
            side_code=1,
            vol=qty,
            order_type=order_type,
            price=price,
            leverage=leverage,
            client_order_id=client_oid,
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
        sym = self.spot_like_symbol_to_mexc(symbol)
        # side_code: 3 = open short
        return await self._submit_order(
            symbol_mexc=sym,
            side_code=3,
            vol=qty,
            order_type=order_type,
            price=price,
            leverage=leverage,
            client_order_id=client_oid,
        )

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
        order_type: Literal["market","limit"]="market",
        price: Optional[str]=None,
        leverage: Optional[int | str]=None,
        client_oid: Optional[str]=None,
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

    # ---------------------------------
    # position helpers
    # ---------------------------------
    async def _fetch_open_positions_raw(self, symbol_mexc: Optional[str]=None) -> List[Dict[str, Any]]:
        """
        GET /api/v1/private/position/open_positions[?symbol=...]  (приватный GET) :contentReference[oaicite:10]{index=10}
        Возвращает массив открытых позиций.
        """
        params: Dict[str, Any] = {}
        if symbol_mexc:
            params["symbol"] = symbol_mexc
        data = await self._get_private("/api/v1/private/position/open_positions", params)

        arr = data.get("data")
        if arr is None:
            return []
        if isinstance(arr, list):
            return arr
        if isinstance(arr, dict):
            return [arr]
        return []

    async def _get_side_size(
        self,
        symbol_mexc: str,
        want_side: Literal["long","short"]
    ) -> Tuple[Decimal, Optional[Dict[str, Any]]]:
        """
        Возвращаем (объём_контракта, сырая_позиция) по конкретной стороне.
        MEXC: positionType == 1 (long) / 2 (short). :contentReference[oaicite:11]{index=11}
        """
        want_side = want_side.lower()
        raw_positions = await self._fetch_open_positions_raw(symbol_mexc)

        for p in raw_positions:
            pos_type_val = p.get("positionType")
            side_txt = "long" if str(pos_type_val) == "1" else "short"
            if side_txt != want_side:
                continue

            vol_str = str(
                p.get("holdVol")
                or p.get("availableVol")
                or p.get("availVol")
                or p.get("vol")
                or "0"
            )
            vol_dec = _d(vol_str)
            if vol_dec > 0:
                return vol_dec, p
        return _d(0), None

    async def _contract_step_info(self, symbol_mexc: str) -> Tuple[Decimal, Decimal]:
        """
        Возвращает (min_vol, step) чтобы корректно округлить количество.
        Берём minVol и volScale/volPrecision. :contentReference[oaicite:12]{index=12}
        """
        info = await self._get_contract_detail(symbol_mexc)
        min_vol = _d(info.get("minVol", "0"))

        vol_scale = info.get("volScale", info.get("volPrecision", 0))
        vol_scale = int(vol_scale or 0)
        step = _d("1") / (_d(10) ** vol_scale) if vol_scale > 0 else _d("0")

        return min_vol, step

    # ---------------------------------
    # closing helpers
    # ---------------------------------
    async def close_long_all(self, symbol: str) -> Optional[Dict[str, Any]]:
        """
        Если у нас есть long по symbol -> отправляем маркет "close long"
        side_code для close long = 4. :contentReference[oaicite:13]{index=13}
        """
        sym = self.spot_like_symbol_to_mexc(symbol)
        size, pos_raw = await self._get_side_size(sym, "long")
        if size <= 0:
            return None

        min_vol, step = await self._contract_step_info(sym)
        qty = size
        if step > 0:
            qty = _round_step(qty, step)
        if qty < min_vol:
            qty = min_vol

        position_id = pos_raw.get("positionId") if pos_raw else None

        return await self._submit_order(
            symbol_mexc=sym,
            side_code=4,  # close long
            vol=_trim_decimals(qty),
            order_type="market",
            leverage=None,
            position_id=position_id,
        )

    async def close_short_all(self, symbol: str) -> Optional[Dict[str, Any]]:
        """
        Если у нас есть short по symbol -> отправляем маркет "close short"
        side_code для close short = 2. :contentReference[oaicite:14]{index=14}
        """
        sym = self.spot_like_symbol_to_mexc(symbol)
        size, pos_raw = await self._get_side_size(sym, "short")
        if size <= 0:
            return None

        min_vol, step = await self._contract_step_info(sym)
        qty = size
        if step > 0:
            qty = _round_step(qty, step)
        if qty < min_vol:
            qty = min_vol

        position_id = pos_raw.get("positionId") if pos_raw else None

        return await self._submit_order(
            symbol_mexc=sym,
            side_code=2,  # close short
            vol=_trim_decimals(qty),
            order_type="market",
            leverage=None,
            position_id=position_id,
        )

    async def close_all_positions(self, symbol: str) -> Dict[str, Optional[Dict[str, Any]]]:
        """
        Закрывает и long, и short по инструменту.
        """
        res_long  = await self.close_long_all(symbol)
        res_short = await self.close_short_all(symbol)
        return {"long_closed": res_long, "short_closed": res_short}

    # ---------------------------------
    # public wrapper for UI/debug (как в Bitget)
    # ---------------------------------
    async def get_open_positions(
        self,
        *,
        symbol: Optional[str] = None,
    ) -> Optional[List[Dict[str, Any]]]:
        """
        Возвращает унифицированный список позиций, похожий на Bitget.get_open_positions():

        {
            "opened_at": ISO-UTC,
            "coin": "BTC",
            "type": "long"|"short",
            "usdt": "123.45",     # оценка markPrice * size
            "leverage": "5",
            "pnl": "3.21"         # приблизительный uPnL
        }

        uPnL считаем сами: (lastPrice - avgPrice)*vol для long,
                           (avgPrice - lastPrice)*vol для short.
        Это приближённо (без комиссий и funding).
        """
        sym_for_fetch = self.spot_like_symbol_to_mexc(symbol) if symbol else None
        raw_positions = await self._fetch_open_positions_raw(sym_for_fetch)

        out: List[Dict[str, Any]] = []

        for p in raw_positions:
            hold_vol = _d(p.get("holdVol") or "0")
            if hold_vol <= 0:
                continue

            sym = (p.get("symbol") or "").upper()
            if symbol:
                want = self.spot_like_symbol_to_mexc(symbol).upper()
                if sym != want:
                    continue

            pos_type_val = p.get("positionType")
            side_txt = "long" if str(pos_type_val) == "1" else "short"

            avg_price = _d(p.get("holdAvgPrice") or p.get("openAvgPrice") or "0")

            # последний марк/текущая цена
            t = await self._get_ticker(sym)
            last_price = _d(
                t.get("lastPrice")
                or t.get("fairPrice")
                or t.get("indexPrice")
                or "0"
            )

            # оценка позиции в USDT
            position_value = _d("0")
            if last_price > 0:
                position_value = last_price * hold_vol

            # примерный unrealized pnl
            pnl_dec = _d("0")
            try:
                if last_price > 0 and avg_price > 0:
                    if side_txt == "long":
                        pnl_dec = (last_price - avg_price) * hold_vol
                    else:  # short
                        pnl_dec = (avg_price - last_price) * hold_vol
            except Exception:
                pass

            # coin из символа 'BTC_USDT' -> 'BTC'
            coin = sym.replace("_USDT", "") if sym.endswith("_USDT") else sym

            lev = str(p.get("leverage") or self.default_leverage)

            opened_at = _to_iso_ms(
                p.get("createTime")
                or p.get("openTime")
                or p.get("positionCreateTime")
            )

            out.append({
                "opened_at": opened_at,
                "coin": coin,
                "type": side_txt,
                "usdt": _trim_decimals(position_value.normalize()) if position_value > 0 else "0",
                "leverage": lev,
                "pnl": _trim_decimals(pnl_dec.normalize()) if pnl_dec != 0 else "0",
            })

        return out or None


# ------------------------------
# пример использования
# ------------------------------
async def main():
    symbol = "BIOUSDT"  # станет 'BIO_USDT'

    async with MEXCAsyncFuturesClient(
        MEXC_API_KEY,
        MEXC_API_SECRET,
        default_margin_mode="isolated",  # или "cross"
        default_leverage=5,
    ) as client:

        # открыть/шортануть:
        await client.open_long_usdt(symbol, 10, leverage=5)
        # await client.open_short_usdt(symbol, 20, leverage=5)

        positions = await client.get_open_positions(symbol=symbol)
        print("OPEN POSITIONS:", positions)

        # res = await client.close_all_positions(symbol)
        # print("CLOSE ALL:", res)


if __name__ == "__main__":
    asyncio.run(main())
