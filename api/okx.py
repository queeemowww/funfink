# okx_async_futures_mainnet.py
from __future__ import annotations

import os
import time
import json
import hmac
import base64
import hashlib
import asyncio
import random
import string
from decimal import Decimal, ROUND_DOWN
from typing import Optional, Literal, Dict, Any, List, Tuple
import ssl
import httpx
from dotenv import load_dotenv
# --- в начале файла рядом с импортами ---
import math
import random as _random
from httpx import ConnectTimeout, ReadTimeout, ConnectError, RemoteProtocolError, HTTPStatusError


SSL_CTX = ssl.create_default_context()
RETRYABLE_STATUS = {429, 500, 502, 503, 504}
MAX_TRIES = 4  # 1 + 3 повтора
BASE_BACKOFF = 0.35  # сек, потом *2, + небольшие джиттеры

def _backoff_sleep(attempt: int) -> float:
    # attempt: 1..N
    # 0.35, 0.7, 1.4 ... + jitter (0..0.2)
    return BASE_BACKOFF * (2 ** (attempt - 1)) + _random.uniform(0.0, 0.2)



load_dotenv()

OKX_API_KEY        = os.getenv("OKX_API_KEY", "")
OKX_API_SECRET     = os.getenv("OKX_API_SECRET", "")       # RAW secret строкой
OKX_API_PASSPHRASE = os.getenv("OKX_API_PASSPHRASE", "")
OKX_SIMULATED      = os.getenv("OKX_SIMULATED", "0")       # "1" -> paper trading header


# ------------------ helpers ------------------
ALNUM = string.ascii_letters + string.digits

def _d(x) -> Decimal:
    return Decimal(str(x))

def _round_step(value: Decimal, step: Decimal) -> Decimal:
    if step == 0:
        return value
    q = (value / step).to_integral_value(rounding=ROUND_DOWN)
    return q * step

def _fmt_decimal(x: Decimal) -> str:
    s = format(x, "f").rstrip("0").rstrip(".")
    return s if s else "0"

def _safe_clordid(prefix: str = "o") -> str:
    """
    OKX clOrdId: только [A-Za-z0-9], длина <= 32, лучше начинать с буквы.
    """
    prefix = "".join(ch for ch in prefix if ch.isalnum())
    if not prefix or not prefix[0].isalpha():
        prefix = "o"
    rnd_len = max(1, 32 - len(prefix))
    rnd = "".join(random.choice(ALNUM) for _ in range(rnd_len))
    return (prefix + rnd)[:32]

def to_okx_inst_id(coin_usdt: str) -> str:
    """
    'BIOUSDT' -> 'BIO-USDT-SWAP'; если уже 'BIO-USDT-SWAP' — вернёт как есть.
    """
    s = coin_usdt.strip().upper()
    if "-" in s:
        return s
    if not s.endswith("USDT"):
        raise ValueError("Ожидался символ вида XXXUSDT или готовый instId XXX-USDT-SWAP")
    base = s[:-4]
    return f"{base}-USDT-SWAP"


class OKXAsyncClient:
    """
    OKX v5 клиент для USDT-маржинальных SWAP (без ccxt).
    """
    def __init__(self, api_key: str, api_secret: str, passphrase: str, *, timeout: float = 15.0):
        self.api_key = api_key
        self.api_secret = api_secret.encode("utf-8")  # байты для HMAC
        self.passphrase = passphrase
        self.base_url = "https://www.okx.com"
        # --- В __init__ клиента: замените создание AsyncClient ---
        limits = httpx.Limits(max_connections=100, max_keepalive_connections=30)
        timeout = httpx.Timeout(connect=5.0, read=20.0, write=20.0, pool=5.0)  # <— плотнее, чем дефолт
        headers = {"Content-Type": "application/json"}
        if OKX_SIMULATED == "1":
            headers["x-simulated-trading"] = "1"

        self._client = httpx.AsyncClient(
            base_url=self.base_url,
            timeout=timeout,
            headers=headers,
            limits=limits,
            http2=True,
            verify=SSL_CTX
        )

    async def close(self):
        await self._client.aclose()

    # ------------------ подпись и приватные запросы ------------------
    @staticmethod
    def _ts_iso() -> str:
        t = time.time()
        return time.strftime('%Y-%m-%dT%H:%M:%S', time.gmtime(t)) + f".{int((t%1)*1000):03d}Z"

    def _sign(self, ts: str, method: str, path_with_query: str, body: str = "") -> str:
        prehash = f"{ts}{method}{path_with_query}{body}".encode("utf-8")
        mac = hmac.new(self.api_secret, prehash, hashlib.sha256).digest()
        return base64.b64encode(mac).decode()

    # --- ДОБАВЬ новый универсальный паблик GET с ретраями ---
    async def _get_public(self, path: str, params: Dict[str, Any]) -> Dict[str, Any]:
        last_exc = None
        for attempt in range(1, MAX_TRIES + 1):
            try:
                r = await self._client.get(path, params=params or {})
                # вручную проверим статусы для ретраев
                if r.status_code in RETRYABLE_STATUS:
                    raise HTTPStatusError("retryable status", request=r.request, response=r)
                r.raise_for_status()
                data = r.json()
                return data
            except (ConnectTimeout, ReadTimeout, ConnectError, RemoteProtocolError, HTTPStatusError) as e:
                last_exc = e
                if attempt >= MAX_TRIES:
                    raise
                await asyncio.sleep(_backoff_sleep(attempt))
        # на всякий
        if last_exc:
            raise last_exc


    # --- ПЕРЕПИШИ _request_private: ретраи + подпиcь на каждую попытку ---
    async def _request_private(
        self,
        method: Literal["GET", "POST"],
        path: str,
        *,
        params: Dict[str, Any] | None = None,
        body: Dict[str, Any] | None = None,
    ) -> Dict[str, Any]:
        last_exc = None
        for attempt in range(1, MAX_TRIES + 1):
            try:
                ts = self._ts_iso()
                if method == "GET":
                    body_or_query = ""
                    if params:
                        from urllib.parse import urlencode
                        qs = "?" + urlencode(params, doseq=True)
                        body_or_query = qs
                    sign = self._sign(ts, method, path + body_or_query, "")
                    headers = {
                        "OK-ACCESS-KEY": self.api_key,
                        "OK-ACCESS-SIGN": sign,
                        "OK-ACCESS-TIMESTAMP": ts,
                        "OK-ACCESS-PASSPHRASE": self.passphrase,
                    }
                    if OKX_SIMULATED == "1":
                        headers["x-simulated-trading"] = "1"
                    r = await self._client.get(path, headers=headers, params=params or {})
                else:
                    payload = json.dumps(body or {}, separators=(",", ":"), ensure_ascii=False)
                    sign = self._sign(ts, method, path, payload)
                    headers = {
                        "OK-ACCESS-KEY": self.api_key,
                        "OK-ACCESS-SIGN": sign,
                        "OK-ACCESS-TIMESTAMP": ts,
                        "OK-ACCESS-PASSPHRASE": self.passphrase,
                        "Content-Type": "application/json",
                    }
                    if OKX_SIMULATED == "1":
                        headers["x-simulated-trading"] = "1"
                    r = await self._client.post(path, headers=headers, content=payload)

                if r.status_code in RETRYABLE_STATUS:
                    raise HTTPStatusError("retryable status", request=r.request, response=r)
                r.raise_for_status()
                data = r.json()
                if data.get("code") != "0":
                    # функциональные ошибки биржи не ретраим
                    raise RuntimeError(f"OKX error: {data.get('code')} {data.get('msg')} | {data}")
                return data

            except (ConnectTimeout, ReadTimeout, ConnectError, RemoteProtocolError, HTTPStatusError) as e:
                last_exc = e
                if attempt >= MAX_TRIES:
                    raise
                await asyncio.sleep(_backoff_sleep(attempt))
        if last_exc:
            raise last_exc


    async def _post_private(self, path: str, body: Dict[str, Any]) -> Dict[str, Any]:
        return await self._request_private("POST", path, body=body)

    async def _post_order_with_retry(self, body: Dict[str, Any]) -> Dict[str, Any]:
        try:
            return await self._post_private("/api/v5/trade/order", body)
        except RuntimeError as e:
            msg = str(e)
            # иногда OKX ругается на кастомный clOrdId — повторим без него
            if "clOrdId" in msg and ("Parameter" in msg or "51000" in msg):
                b2 = dict(body); b2.pop("clOrdId", None)
                return await self._post_private("/api/v5/trade/order", b2)
            raise

    # --- Обнови публичные методы на общий ретрайный _get_public ---
    async def _get_ticker(self, inst_id: str) -> Dict[str, Any]:
        data = await self._get_public("/api/v5/market/ticker", {"instId": inst_id})
        if data.get("code") != "0" or not data.get("data"):
            raise RuntimeError(f"Ticker error: {data}")
        return data["data"][0]

    async def _get_instrument(self, inst_id: str) -> Dict[str, Any]:
        data = await self._get_public("/api/v5/public/instruments", {"instType": "SWAP", "instId": inst_id})
        if data.get("code") != "0" or not data.get("data"):
            raise RuntimeError(f"Instrument error: {data}")
        return data["data"][0]


    # ------------------ аккаунт: posMode (net vs hedge) ------------------
    async def _get_account_config(self) -> Dict[str, Any]:
        data = await self._request_private("GET", "/api/v5/account/config")
        lst = data.get("data") or []
        if not lst:
            raise RuntimeError(f"Account config error: {data}")
        return lst[0]

    async def _get_pos_mode(self) -> str:
        """
        'net_mode' (one-way) или 'long_short_mode' (hedge).
        """
        cfg = await self._get_account_config()
        return cfg.get("posMode", "net_mode")

    # ------------------ ЛЕВЕРИДЖ ------------------
    async def set_leverage(
        self,
        inst_id: str,
        lever: int | str,
        *,
        td_mode: Literal["cross", "isolated"],
        pos_side: Optional[Literal["long", "short"]] = None,
    ) -> Dict[str, Any]:
        """
        Устанавливает плечо для инструмента. На OKX плечо задаётся отдельно от ордера.
        В hedge-режиме (long_short_mode) желательно указывать posSide.
        """
        body: Dict[str, Any] = {
            "instId": inst_id,
            "lever": str(lever),
            "mgnMode": td_mode,
        }
        # posSide разрешён только в hedge-режиме
        if pos_side and (await self._get_pos_mode()) == "long_short_mode":
            body["posSide"] = pos_side
        return await self._post_private("/api/v5/account/set-leverage", body)

    # ------------------ конвертация USDT -> sz (контракты) ------------------
    async def usdt_to_qty(
        self,
        symbol,
        usdt_amount: float | str,
        *,
        side: Literal["buy", "sell"],  # buy -> askPx, sell -> bidPx
    ) -> str:
        inst_id = to_okx_inst_id(symbol)
        t = await self._get_ticker(inst_id)
        px_str = t.get("askPx") if side == "buy" else t.get("bidPx")
        if not px_str or _d(px_str) == 0:
            px_str = t.get("last") or t.get("sodUtc0")
        price = _d(px_str)
        if price <= 0:
            raise RuntimeError(f"Bad price for {inst_id}: {price}")

        ins = await self._get_instrument(inst_id)
        self.contract_size = _d(ins.get("ctVal", "1"))
        lot_sz = _d(ins.get("lotSz", "1"))
        min_sz = _d(ins.get("minSz", "1"))

        usdt = _d(usdt_amount)
        coin_qty = usdt / price
        raw_sz = coin_qty / (self.contract_size if self.contract_size > 0 else _d(1))

        sz = raw_sz
        if lot_sz > 0:
            sz = _round_step(sz, lot_sz)
        if sz < min_sz:
            sz = min_sz

        return str(sz * self.contract_size)

    # ------------------ позиции: поиск фактической ------------------
    async def _find_position(
        self, inst_id: str
    ) -> Tuple[str, Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
        """
        Возвращает (pos_mode, long_pos, short_pos) для inst_id.
        В net_mode одна из них будет None, а актуальная — в long_pos (qty>0) или short_pos (qty<0).
        """
        raw = await self._request_private("GET", "/api/v5/account/positions", params={"instType": "SWAP", "instId": inst_id})
        items = raw.get("data") or []
        pos_mode = await self._get_pos_mode()

        long_pos = None
        short_pos = None

        for p in items:
            if p.get("instId") != inst_id:
                continue
            qty = _d(p.get("pos", "0"))
            if qty == 0:
                continue

            if pos_mode == "long_short_mode":
                side = (p.get("posSide") or "").lower()
                if side == "long" and qty > 0:
                    long_pos = p
                elif side == "short" and qty < 0:
                    short_pos = p
            else:
                # net mode: знак qty определяет сторону
                if qty > 0:
                    long_pos = p
                elif qty < 0:
                    short_pos = p

        return pos_mode, long_pos, short_pos

    # ------------------ билдер тела ордера ------------------
    async def _build_order_body(
        self,
        *,
        inst_id: str,
        side: Literal["buy", "sell"],
        ord_type: Literal["market", "limit"],
        sz: str,
        td_mode: Literal["cross", "isolated"],
        px: Optional[str],
        reduce_only: bool,
        pos_side: Optional[Literal["long", "short"]],
        cl_ord_id: Optional[str],
        tag: Optional[str],
    ) -> Dict[str, Any]:
        pos_mode = await self._get_pos_mode()

        body: Dict[str, Any] = {
            "instId": inst_id,
            "tdMode": td_mode,
            "side": side,
            "ordType": ord_type,
            "sz": sz,
            "reduceOnly": str(reduce_only).lower(),
        }
        if cl_ord_id:
            body["clOrdId"] = cl_ord_id
        if tag:
            body["tag"] = tag
        if ord_type == "limit":
            if not px:
                raise ValueError("px is required for limit orders")
            body["px"] = px

        # posSide — только в hedge-режиме
        if pos_mode == "long_short_mode" and pos_side:
            body["posSide"] = pos_side

        return body

    # ------------------ низкоуровневые ордера (sz уже посчитан) ------------------
    async def open_long(
        self,
        symbol: str,
        qty: str,
        *,
        ord_type: Literal["market", "limit"] = "market",
        px: Optional[str] = None,
        td_mode: Literal["cross", "isolated"] = "cross",
        pos_side: Optional[Literal["long", "short"]] = None,   # игнорируется в net_mode
        reduce_only: bool = False,
        cl_ord_id: Optional[str] = None,
        tag: Optional[str] = None,
        leverage: Optional[int | str] = None,
    ) -> Dict[str, Any]:
        inst_id = to_okx_inst_id(symbol)
        await self.usdt_to_qty(symbol=symbol, usdt_amount=100, side="buy")
        if leverage is not None:
            mode = await self._get_pos_mode()
            eff_pos_side = pos_side if (mode == "long_short_mode") else None
            if eff_pos_side is None and mode == "long_short_mode":
                eff_pos_side = "long"
            await self.set_leverage(inst_id, leverage, td_mode=td_mode, pos_side=eff_pos_side)

        if cl_ord_id is None:
            cl_ord_id = _safe_clordid("oL")
        body = await self._build_order_body(
            inst_id=inst_id, side="buy", ord_type=ord_type, sz=str(int(float(qty)/ float(self.contract_size))), td_mode=td_mode,
            px=px, reduce_only=reduce_only, pos_side=pos_side, cl_ord_id=cl_ord_id, tag=tag,
        )
        return await self._post_order_with_retry(body)

    async def open_short(
        self,
        symbol: str,
        qty: str,
        *,
        ord_type: Literal["market", "limit"] = "market",
        px: Optional[str] = None,
        td_mode: Literal["cross", "isolated"] = "cross",
        pos_side: Optional[Literal["long", "short"]] = None,   # игнорируется в net_mode
        reduce_only: bool = False,
        cl_ord_id: Optional[str] = None,
        tag: Optional[str] = None,
        leverage: Optional[int | str] = None,
    ) -> Dict[str, Any]:
        await self.usdt_to_qty(symbol=symbol, usdt_amount=100, side="sell")
        inst_id = to_okx_inst_id(symbol)
        if leverage is not None:
            mode = await self._get_pos_mode()
            eff_pos_side = pos_side if (mode == "long_short_mode") else None
            if eff_pos_side is None and mode == "long_short_mode":
                eff_pos_side = "short"
            await self.set_leverage(inst_id, leverage, td_mode=td_mode, pos_side=eff_pos_side)

        if cl_ord_id is None:
            cl_ord_id = _safe_clordid("oS")
        body = await self._build_order_body(
            inst_id=inst_id, side="sell", ord_type=ord_type, sz=str(int(float(qty) / float(self.contract_size))), td_mode=td_mode,
            px=px, reduce_only=reduce_only, pos_side=pos_side, cl_ord_id=cl_ord_id, tag=tag,
        )
        return await self._post_order_with_retry(body)

    # ------------------ ВНЕШНЯЯ ПУБЛИЧНАЯ ФУНКЦИЯ ПОЛНОГО ЗАКРЫТИЯ ------------------
        # -------- lot/minSz для квантования sz --------
    async def _get_lot_filters(self, inst_id: str) -> tuple[Decimal, Decimal]:
        ins = await self._get_instrument(inst_id)
        lot_sz = _d(ins.get("lotSz", "1") or "1")
        min_sz = _d(ins.get("minSz", "1") or "1")
        return lot_sz, min_sz

    # -------- закрытие ВЕСЬ ЛОНГ --------
    async def close_long_all(
        self,
        symbol: str,
        *,
        cl_ord_id_prefix: str = "cL",
        tag: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """
        Закрывает ВЕСЬ лонг по symbol (если есть). Возвращает ответ OKX или None.
        """
        inst_id = to_okx_inst_id(symbol)
        pos_mode, long_pos, _ = await self._find_position(inst_id)
        if not long_pos:
            return None

        mgn_mode = (long_pos.get("mgnMode") or "").lower() or "cross"
        pos_side = None
        if pos_mode == "long_short_mode":
            pos_side = "long"

        # 1) официальный полный close
        body = {"instId": inst_id, "mgnMode": mgn_mode}
        if pos_side:
            body["posSide"] = pos_side
        try:
            return await self._post_private("/api/v5/trade/close-position", body)
        except Exception:
            # 2) фолбэк: рыночный reduceOnly SELL на весь объём
            qty_abs = _d(long_pos.get("pos", "0")).copy_abs()
            if qty_abs <= 0:
                return None

            lot_sz, min_sz = await self._get_lot_filters(inst_id)
            # квантуем под шаг/минимум
            if lot_sz > 0:
                qty_abs = _round_step(qty_abs, lot_sz)
            if qty_abs < min_sz:
                qty_abs = min_sz

            order = {
                "instId": inst_id,
                "tdMode": mgn_mode,
                "side": "sell",
                "ordType": "market",
                "sz": _fmt_decimal(qty_abs),
                "reduceOnly": "true",
                "clOrdId": _safe_clordid(cl_ord_id_prefix),
            }
            if tag:
                order["tag"] = tag
            if pos_side:
                order["posSide"] = "long"   # закрываем лонг продажей в hedge-режиме
            return await self._post_order_with_retry(order)

    # -------- закрытие ВЕСЬ ШОРТ --------
    async def close_short_all(
        self,
        symbol: str,
        *,
        cl_ord_id_prefix: str = "cS",
        tag: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """
        Закрывает ВЕСЬ шорт по symbol (если есть). Возвращает ответ OKX или None.
        """
        inst_id = to_okx_inst_id(symbol)
        pos_mode, _, short_pos = await self._find_position(inst_id)
        if not short_pos:
            return None

        mgn_mode = (short_pos.get("mgnMode") or "").lower() or "cross"
        pos_side = None
        if pos_mode == "long_short_mode":
            pos_side = "short"

        # 1) официальный полный close
        body = {"instId": inst_id, "mgnMode": mgn_mode}
        if pos_side:
            body["posSide"] = pos_side
        try:
            return await self._post_private("/api/v5/trade/close-position", body)
        except Exception:
            # 2) фолбэк: рыночный reduceOnly BUY на весь объём
            qty_abs = _d(short_pos.get("pos", "0")).copy_abs()
            if qty_abs <= 0:
                return None

            lot_sz, min_sz = await self._get_lot_filters(inst_id)
            if lot_sz > 0:
                qty_abs = _round_step(qty_abs, lot_sz)
            if qty_abs < min_sz:
                qty_abs = min_sz

            order = {
                "instId": inst_id,
                "tdMode": mgn_mode,
                "side": "buy",
                "ordType": "market",
                "sz": _fmt_decimal(qty_abs),
                "reduceOnly": "true",
                "clOrdId": _safe_clordid(cl_ord_id_prefix),
            }
            if tag:
                order["tag"] = tag
            if pos_side:
                order["posSide"] = "short"  # закрываем шорт покупкой в hedge-режиме
            return await self._post_order_with_retry(order)

    # -------- «bybit-style» обёртка: закрыть обе стороны --------
    async def close_all_positions(
        self,
        symbol: str,
        *,
        cl_ord_id_prefix: str = "cALL",
        tag: Optional[str] = None,
    ) -> Dict[str, Optional[Dict[str, Any]]]:
        """
        Закрывает обе стороны по инструменту (если есть).
        Возвращает {"long_closed": <resp|None>, "short_closed": <resp|None>}.
        """
        inst_id = to_okx_inst_id(symbol)
        # быстрый просмотр: если вообще ничего нет — сразу вернём оба None
        _, long_pos, short_pos = await self._find_position(inst_id)
        if not long_pos and not short_pos:
            return {"long_closed": None, "short_closed": None}

        res_long = await self.close_long_all(
            symbol,
            cl_ord_id_prefix=(cl_ord_id_prefix + "L"),
            tag=tag,
        ) if long_pos else None

        res_short = await self.close_short_all(
            symbol,
            cl_ord_id_prefix=(cl_ord_id_prefix + "S"),
            tag=tag,
        ) if short_pos else None

        return {"long_closed": res_long, "short_closed": res_short}


    # ------------------ открытие по USDT ------------------
    async def open_long_usdt(
        self,
        symbol: str,                    # 'BIOUSDT' или 'BIO-USDT-SWAP'
        usdt_amount: float | str,
        *,
        ord_type: Literal["market", "limit"] = "market",
        px: Optional[str] = None,
        td_mode: Literal["cross", "isolated"] = "cross",
        cl_ord_id: Optional[str] = None,
        tag: Optional[str] = None,
        leverage: Optional[int | str] = None,
    ) -> Dict[str, Any]:
        """
        Открыть ЛОНГ на ~usdt_amount USDT эквивалента.
        Автоконвертация USDT -> контрактный размер (sz) с учётом lotSz/minSz.
        """
        inst_id = to_okx_inst_id(symbol)
        sz = await self.usdt_to_qty(symbol, usdt_amount, side="buy")
        return await self.open_long(
            symbol=symbol,
            qty=sz,
            ord_type=ord_type,
            px=px,
            td_mode=td_mode,
            pos_side=None,          # в net_mode игнорируется; в hedge плечо проставится с posSide='long'
            reduce_only=False,
            cl_ord_id=cl_ord_id,
            tag=tag,
            leverage=leverage,
        )

    async def open_short_usdt(
        self,
        symbol: str,                    # 'BIOUSDT' или 'BIO-USDT-SWAP'
        usdt_amount: float | str,
        *,
        ord_type: Literal["market", "limit"] = "market",
        px: Optional[str] = None,
        td_mode: Literal["cross", "isolated"] = "cross",
        cl_ord_id: Optional[str] = None,
        tag: Optional[str] = None,
        leverage: Optional[int | str] = None,
    ) -> Dict[str, Any]:
        """
        Открыть ШОРТ на ~usdt_amount USDT эквивалента.
        Автоконвертация USDT -> контрактный размер (sz) с учётом lotSz/minSz.
        """
        inst_id = to_okx_inst_id(symbol)
        sz = await self.usdt_to_qty(symbol, usdt_amount, side="sell")
        return await self.open_short(
            symbol=symbol,
            qty=sz,
            ord_type=ord_type,
            px=px,
            td_mode=td_mode,
            pos_side=None,          # в net_mode игнорируется; в hedge плечо проставится с posSide='short'
            reduce_only=False,
            cl_ord_id=cl_ord_id,
            tag=tag,
            leverage=leverage,
        )

    # ------------------ helpers (time) ------------------
    @staticmethod
    def _iso_from_millis(ms_str: str) -> str:
        try:
            ms = int(ms_str)
        except Exception:
            return ""
        return time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime(ms / 1000))

    # ------------------ открытые позиции (сводка) ------------------
    async def get_open_positions(
        self,
        symbol: Optional[str] = None,   # 'BIOUSDT' или 'BIO-USDT-SWAP' или None (все)
    ) -> Optional[List[dict]]:
        params: Dict[str, Any] = {"instType": "SWAP"}
        if symbol:
            params["instId"] = to_okx_inst_id(symbol)

        raw = await self._request_private("GET", "/api/v5/account/positions", params=params)
        items = raw.get("data") or []
        result = {}
        if not items:
            return None

        ticker_cache: dict[str, Dict[str, Any]] = {}
        ins_cache: dict[str, Dict[str, Any]] = {}

        for p in items:
            try:
                inst_id: str = p.get("instId", "")
                if not inst_id or not inst_id.endswith("-USDT-SWAP"):
                    continue

                pos_str = p.get("pos", "0")
                if not pos_str or _d(pos_str) == 0:
                    continue  # пустая позиция

                pos_side = (p.get("posSide") or "").lower()  # "long"/"short"/"net"
                leverage = p.get("lever") or p.get("leverage") or ""
                c_time = p.get("cTime") or p.get("cTimeMs") or ""
                opened_at = self._iso_from_millis(c_time) if c_time else ""

                # сторона (для net_mode — по знаку)
                if pos_side in ("long", "short"):
                    side = pos_side
                else:
                    side = "long" if _d(pos_str) > 0 else "short"

                coin = inst_id.split("-")[0] if "-" in inst_id else ""

                # PnL
                pnl = p.get("upl")
                avg_px = p.get("avgPx")
                mark_px = float(p.get("markPx"))
                liq_px = p.get("liqPx")
                if pnl is None or pnl == "":
                    if inst_id not in ticker_cache:
                        ticker_cache[inst_id] = await self._get_ticker(inst_id)
                    t = ticker_cache[inst_id]
                    last_px = _d(t.get("last") or t.get("askPx") or t.get("bidPx") or "0")

                    avg_px = _d(p.get("avgPx") or "0")
                    if inst_id not in ins_cache:
                        ins_cache[inst_id] = await self._get_instrument(inst_id)
                    ins = ins_cache[inst_id]
                    contract_size = _d(ins.get("ctVal", "1"))

                    contracts = _d(pos_str).copy_abs()
                    sgn = _d(1) if side == "long" else _d(-1)
                    pnl_d = (last_px - avg_px) * contracts * contract_size * sgn
                    pnl = _fmt_decimal(pnl_d)
                else:
                    pnl = _fmt_decimal(_d(pnl))

                # USDT-эквивалент
                notional_usd = p.get("notionalUsd")
                if notional_usd is None or notional_usd == "":
                    if inst_id not in ticker_cache:
                        ticker_cache[inst_id] = await self._get_ticker(inst_id)
                    t = ticker_cache[inst_id]
                    last_px = _d(t.get("last") or t.get("askPx") or t.get("bidPx") or "0")
                    if inst_id not in ins_cache:
                        ins_cache[inst_id] = await self._get_instrument(inst_id)
                    ins = ins_cache[inst_id]
                    contract_size = _d(ins.get("ctVal", "1"))
                    contracts = _d(pos_str).copy_abs()
                    usdt_val = contracts * contract_size * last_px
                    usdt_str = _fmt_decimal(usdt_val)
                else:
                    usdt_str = _fmt_decimal(_d(notional_usd))
                result = {
                    "opened_at": opened_at,
                    "symbol": coin,
                    "side": side,
                    "pnl": pnl,
                    "leverage": str(leverage),
                    "entry_usdt": ((float(usdt_str) - float(pnl)) / float(leverage)),
                    "entry_price": avg_px, 
                    "market_price": mark_px,
                    "liq_price": 0,
                }
            except Exception:
                continue

        return result or None
    
        # ------------------ баланс аккаунта (USDT) ------------------
    # ------------------ общий баланс по USDT (все аккаунты) ------------------
    async def get_usdt_balance(self) -> str:
        """
        Возвращает СУММАРНЫЙ доступный баланс в USDT (строкой) по всем аккаунтам,
        к которым есть доступ у текущего API-ключа:
          * основной trading/unified аккаунт (account/balance),
          * основной funding аккаунт (asset/balances),
          * все субаккаунты (trading + funding), если ключ является мастер-аккаунтом.

        Приоритет полей для trading/unified:
          1) availBal
          2) availEq
          3) cashBal
          4) eq

        Для funding используем:
          * availBal, если есть, иначе bal.
        """
        total = _d("0")

        # ---------- 1) Основной trading / unified аккаунт ----------
        raw = await self._request_private(
            "GET",
            "/api/v5/account/balance",
            params={"ccy": "USDT"},
        )
        data = raw.get("data") or []
        for acc in data:
            details = acc.get("details") or []
            for d in details:
                if (d.get("ccy") or "").upper() != "USDT":
                    continue
                for key in ("availBal", "availEq", "cashBal", "eq"):
                    v = d.get(key)
                    if v not in (None, ""):
                        total += _d(v)
                        break
                # по одной записи на валюту на аккаунт
                break

        # ---------- 2) Основной funding-аккаунт ----------
        try:
            raw_f = await self._request_private(
                "GET",
                "/api/v5/asset/balances",
                params={"ccy": "USDT"},
            )
            dlist = raw_f.get("data") or []
            for d in dlist:
                if (d.get("ccy") or "").upper() != "USDT":
                    continue
                v = d.get("availBal") or d.get("bal")
                if v not in (None, ""):
                    total += _d(v)
                    break
        except Exception:
            # funding может быть недоступен/отключён — просто игнорируем
            pass

        # ---------- 3) Все субаккаунты (если ключ — мастер) ----------
        try:
            raw_sub_list = await self._request_private(
                "GET",
                "/api/v5/users/subaccount/list"
            )
            sub_data = raw_sub_list.get("data") or []
            sub_names = [s.get("subAcct") for s in sub_data if s.get("subAcct")]

            for sub in sub_names:
                # --- trading баланс субаккаунта ---
                try:
                    raw_sub_tr = await self._request_private(
                        "GET",
                        "/api/v5/account/subaccount/balances",
                        params={"subAcct": sub},
                    )
                    tr_data = raw_sub_tr.get("data") or []
                    for acc in tr_data:
                        details = acc.get("details") or []
                        for d in details:
                            if (d.get("ccy") or "").upper() != "USDT":
                                continue
                            for key in ("availBal", "availEq", "cashBal", "eq"):
                                v = d.get(key)
                                if v not in (None, ""):
                                    total += _d(v)
                                    break
                            break
                except Exception:
                    # не получилось прочитать конкретный субакк — пропускаем
                    pass

                # --- funding баланс субаккаунта ---
                try:
                    raw_sub_f = await self._request_private(
                        "GET",
                        "/api/v5/asset/subaccount/balances",
                        params={"subAcct": sub, "ccy": "USDT"},
                    )
                    fd_data = raw_sub_f.get("data") or []
                    for d in fd_data:
                        if (d.get("ccy") or "").upper() != "USDT":
                            continue
                        v = d.get("availBal") or d.get("bal")
                        if v not in (None, ""):
                            total += _d(v)
                            break
                except Exception:
                    pass

        except Exception:
            # если нет доступа к списку субакков — значит, ключ не мастер, просто игнорируем
            pass

        # ---------- фолбэк на totalEq, если ничего не нашли ----------
        if total == 0 and data:
            acc0 = data[0]
            total_eq = acc0.get("totalEq")
            if total_eq not in (None, ""):
                total = _d(total_eq)

        if total == 0:
            raise RuntimeError("OKX balance parse error: no USDT balances found")

        return _fmt_decimal(total)


# # ------------------ пример использования ------------------
async def main():
    client = OKXAsyncClient(OKX_API_KEY, OKX_API_SECRET, OKX_API_PASSPHRASE)
    try:
        symbol = "BIOUSDT"  # или 'BIO-USDT-SWAP'
        # print(await client.open_long(symbol="BIOUSDT", qty="300", leverage=5))

        # print(await client.usdt_to_qty(symbol="SOONUSDT", usdt_amount=90, side="buy"))

        # открыть сделки (пример):
        # r = await client.open_long_usdt(symbol, 300, leverage=5)
        # print("OPEN LONG:", r) 
        # # r = await client.open_short_usdt(symbol, 50, leverage=1)
        # print("OPEN SHORT:", r)

        # pos = await client.get_open_positions(symbol=symbol)
        # print("OPEN POSITIONS:", pos)

        # --- ПОЛНОЕ закрытие одной функцией ---
        # если есть лонг и/или шорт по symbol — закроет полностью найденные стороны
        # r = await client.close_all_positions(symbol=symbol)
        # print("CLOSE ALL SIDES:", r)

        print(float(await client.get_usdt_balance()))
        # print(await client.usdt_to_qty(symbol="BIOUSDT", usdt_amount=50, side="buy"))

    finally:
        await client.close()

if __name__ == "__main__":
    asyncio.run(main())
