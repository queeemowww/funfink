# htx_async_futures_mainnet.py
from __future__ import annotations

import os
import hmac
import base64
import hashlib
import asyncio
import urllib.parse
from typing import Optional, Literal, Dict, Any, List
from decimal import Decimal, ROUND_DOWN, ROUND_FLOOR
from datetime import datetime, timezone
import ssl
import certifi

CUSTOM_CA_BUNDLE = "/root/custom_ca_bundle.pem"

import httpx
from dotenv import load_dotenv
load_dotenv()

SSL_CONTEXT = ssl.create_default_context(cafile=certifi.where())

HTX_API_KEY    = os.getenv("HTX_API_KEY", "")
HTX_API_SECRET = os.getenv("HTX_API_SECRET", "")

# ---------- numeric utils ----------
def _d(x) -> Decimal:
    return Decimal(str(x))

def _round_step(value: Decimal, step: Decimal) -> Decimal:
    if step == 0:
        return value
    q = (value / step).to_integral_value(rounding=ROUND_DOWN)
    return q * step

# ======================================================
#                  HTX (Huobi DM) client
#           Linear-swap (USDT-margined perpetuals)
# ======================================================
class HTXAsyncClient:
    def __init__(
        self,
        api_key: str,
        api_secret: str,
        *,
        base_url: str = "https://api.hbdm.com",
        timeout: float = 15.0,
        default_retry_leverage: int = 5,
    ):
        if not api_key or not api_secret:
            raise ValueError("HTX_API_KEY / HTX_API_SECRET не заданы")

        self.api_key    = api_key
        self.api_secret = api_secret.encode("utf-8")
        self.base_url   = base_url.rstrip("/")

        # 🔥 Полностью отключаем проверку сертификата
        unsafe_ctx = ssl.create_default_context()
        unsafe_ctx.check_hostname = False
        unsafe_ctx.verify_mode = ssl.CERT_NONE

        print("HTXAsyncClient: SSL verification DISABLED for HTX")  # чтобы точно видеть, что этот код отработал

        self._client = httpx.AsyncClient(
            base_url=self.base_url,
            timeout=timeout,
            verify=unsafe_ctx,  # <--- вместо True / certifi / пути к файлу
            trust_env=False,
        )

        self._retry_lev = int(default_retry_leverage)
        self.contract_size = 1

    # --- context ---
    async def __aenter__(self): 
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.close()

    async def close(self):
        await self._client.aclose()

    # --- internals ---
    @staticmethod
    def _ts_iso8601() -> str:
        # Биржа ждёт UTC строку вида "2025-10-23T20:30:46"
        # без миллисекунд, по документации HTX USDT-Margined Contracts.
        return datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")

    @staticmethod
    def _to_iso_ms(ts_ms: int | str | None) -> Optional[str]:
        if not ts_ms:
            return None
        try:
            return datetime.fromtimestamp(int(ts_ms) / 1000, tz=timezone.utc).isoformat()
        except Exception:
            return None

    def _host(self) -> str:
        return urllib.parse.urlparse(self.base_url).netloc

    @staticmethod
    def _err(data: Dict[str, Any]) -> str:
        if data.get("status") == "ok":
            return "ok"
        return f"{data.get('err_code')} - {data.get('err_msg')}"

    @staticmethod
    def _canonical_query(params: Dict[str, Any]) -> str:
        # HTX signature: сортируем по ключу, urlencode
        items = [(k, "" if v is None else str(v)) for k, v in params.items()]
        items.sort(key=lambda kv: kv[0])
        return urllib.parse.urlencode(items, quote_via=urllib.parse.quote)

    def _signed_params(
        self,
        method: str,
        path: str,
        extra: Dict[str, Any] | None = None
    ) -> Dict[str, Any]:
        """
        Сигнатура формата V2:
        HmacSHA256(payload).digest() -> base64
        payload = method + '\n' + host + '\n' + path + '\n' + canonical_query
        """
        q = {
            "AccessKeyId": self.api_key,
            "SignatureMethod": "HmacSHA256",
            "SignatureVersion": "2",
            "Timestamp": self._ts_iso8601(),  # UTC
        }
        if extra:
            q.update({k: v for k, v in extra.items() if v is not None})
        canonical = self._canonical_query(q)
        payload = "\n".join([method.upper(), self._host(), path, canonical]).encode("utf-8")
        sign = base64.b64encode(hmac.new(self.api_secret, payload, hashlib.sha256).digest()).decode()
        q["Signature"] = sign
        return q

    # --- PRIVATE (POST only) ---
    async def _private_post(
        self,
        path: str,
        body: Dict[str, Any] | None = None,
        query: Dict[str, Any] | None = None
    ) -> Dict[str, Any]:
        sp = self._signed_params("POST", path, query or {})
        r = await self._client.post(path, params=sp, json=body or {})
        r.raise_for_status()
        data = r.json()
        if data.get("status") != "ok":
            raise RuntimeError(f"HTX error: {self._err(data)}")
        return data

    # --- PUBLIC helpers ---
    async def _get_contract_info(self, contract_code: str) -> Dict[str, Any]:
        r = await self._client.get(
            "/linear-swap-api/v1/swap_contract_info",
            params={"contract_code": contract_code},
        )
        r.raise_for_status()
        data = r.json()
        if data.get("status") != "ok" or not data.get("data"):
            raise RuntimeError(f"HTX contract_info error: {self._err(data)}")
        return data["data"][0]

    async def _get_ticker(self, contract_code: str) -> Dict[str, Any]:
        r = await self._client.get(
            "/linear-swap-ex/market/detail/merged",
            params={"contract_code": contract_code},
        )
        r.raise_for_status()
        data = r.json()
        if "tick" not in data or not data["tick"]:
            raise RuntimeError("HTX ticker error: empty tick")
        return data["tick"]

    # --- symbol utils ---
    @staticmethod
    def _to_contract_code(symbol: str) -> str:
        """
        "BIOUSDT" -> "BIO-USDT"
        "BTCUSDT" -> "BTC-USDT"
        "ETHUSD"  -> "ETH-USD"
        """
        s = symbol.upper().strip()
        for q in ("USDT", "USDC", "USD"):
            if s.endswith(q):
                return f"{s[:-len(q)]}-{q}"
        return f"{s}-USDT"

    # --- USDT -> volume (int contracts) ---
    async def usdt_to_qty(
        self,
        symbol: str,
        usdt_amount: float | str,
        *,
        side: Literal["buy","sell"],
    ) -> str:
        """
        Конвертация "хочу примерно N USDT позиции" -> "volume (контрактов)"
        volume у HTX должен быть целым числом контрактов.
        """
        code = self._to_contract_code(symbol)
        tick = await self._get_ticker(code)

        # HTX merged/market/detail/merged возвращает поля ask/bid как массивы [price, size] или last_price в tick["close"]
        ask = None
        bid = None
        if "ask" in tick and tick["ask"]:
            ask = _d(str(tick["ask"][0]))
        if "bid" in tick and tick["bid"]:
            bid = _d(str(tick["bid"][0]))

        price = ask if side == "buy" else bid
        if price is None:
            price = _d(str(tick.get("close") or "0"))
        if price <= 0:
            raise RuntimeError(f"Bad price for {code}: {price}")

        info = await self._get_contract_info(code)
        self.contract_size = _d(str(info.get("contract_size", "1")))
        min_vol = _d(str(info.get("min_volume")
                         or info.get("min_order_volume")
                         or "1"))
        if self.contract_size <= 0:
            self.contract_size = _d(1)

        usdt = _d(usdt_amount)
        self.contract_size = int(self.contract_size)
        # кол-во контрактов = usdt / (price * contract_size)
        contracts = (usdt / (price * self.contract_size)) if price > 0 else _d(0)

        vol_int = int(contracts.to_integral_value(rounding=ROUND_FLOOR))
        if vol_int < int(min_vol):
            vol_int = int(min_vol)
        return str(vol_int * self.contract_size) 

    # --- leverage ---
    async def set_leverage(self, symbol: str, leverage: int | str) -> Dict[str, Any]:
        """
        На HTX плечо задаётся через /linear-swap-api/v1/swap_switch_lever_rate.
        Нужен contract_code и lever_rate.
        """
        code = self._to_contract_code(symbol)
        body = {
            "contract_code": code,
            "lever_rate": int(leverage),
        }
        return await self._private_post(
            "/linear-swap-api/v1/swap_switch_lever_rate",
            body=body,
        )

    # --- core order placement ---
    async def _place_order(
        self,
        *,
        contract_code: str,
        direction: Literal["buy","sell"],
        offset: Literal["open","close"],
        volume: str,
        order_type: Literal["Market","Limit"] = "Market",
        price: Optional[str] = None,
        leverage: Optional[int | str] = None,
        client_order_id: Optional[str] = None,
        _retry_on_1094: bool = True,
    ) -> Dict[str, Any]:
        """
        Универсальный сабмит ордера swap_order.

        Важные моменты:
        - Для маркет-ордеров HTX хочет order_price_type="opponent"
          (best bid/ask исполнение).
        - lever_rate обязателен. Если мы не передадим lever_rate и биржа ответит
          ошибкой 1094 ("Leverage cannot be empty"), мы автоматически
          ретраем с self._retry_lev. Это поведение нужно и при закрытии. :contentReference[oaicite:5]{index=5}
        """
        # если нам явно дали плечо — сначала установим плечо через swap_switch_lever_rate
        # и потом положим его же в тело запроса
        if leverage is not None:
            await self.set_leverage(contract_code.replace("-USDT", "USDT"), int(leverage))

        order_price_type = "opponent" if order_type == "Market" else "limit"
        print("volume ", volume)
        print(int(int(float(volume))), int(float(volume)))

        body: Dict[str, Any] = {
            "contract_code": contract_code,
            "direction": direction,    # "buy" или "sell"
            "offset": offset,          # "open" или "close"
            "volume": int(int(float(volume))),     # целые контракты
            "order_price_type": order_price_type,
        }

        if order_type == "Limit":
            if not price:
                raise ValueError("price is required for Limit orders")
            body["price"] = str(price)

        if client_order_id:
            body["client_order_id"] = client_order_id

        if leverage is not None:
            body["lever_rate"] = int(leverage)

        try:
            return await self._private_post(
                "/linear-swap-api/v1/swap_order",
                body=body,
            )

        except RuntimeError as e:
            msg = str(e)
            # Если биржа говорит "Leverage cannot be empty" (код 1094),
            # мы ретраем с default leverage.
            need_retry = (" 1094 " in msg) or ("1094-" in msg) or ("1094" in msg and "Leverage" in msg)
            if _retry_on_1094 and need_retry and leverage is None:
                default_lev = self._retry_lev
                # выставим плечо для контракта
                await self.set_leverage(contract_code.replace("-USDT", "USDT"), default_lev)

                body_retry = dict(body)
                body_retry["lever_rate"] = default_lev

                return await self._private_post(
                    "/linear-swap-api/v1/swap_order",
                    body=body_retry,
                )
            raise

    # --- internal helper: доступный объём позиции ---
    async def _get_available_position_volume(
        self,
        symbol: str,
        direction: Literal["buy","sell"],
    ) -> int | None:
        """
        Возвращает СКОЛЬКО КОНТРАКТОВ МОЖНО ЗАКРЫТЬ ПРЯМО СЕЙЧАС
        по данному направлению ('buy' = лонг, 'sell' = шорт в их терминах).

        Мы берём поле "available" из swap_position_info,
        а не "volume". Это критично, иначе ловим 1048.  :contentReference[oaicite:6]{index=6}
        """
        code = self._to_contract_code(symbol)
        data = await self._private_post(
            "/linear-swap-api/v1/swap_position_info",
            body={"contract_code": code},
        )
        items = data.get("data") or []

        total_avail = 0
        for p in items:
            if (p.get("contract_code") or "").upper() != code:
                continue
            if (p.get("direction") or "").lower() != direction:
                continue

            # HTX ответ обычно имеет "volume" (общий размер позиции)
            # и "available" (доступно к закрытию).
            avail_raw = p.get("available", p.get("available_volume", p.get("available_position")))
            if avail_raw is None:
                avail_raw = p.get("volume")  # fallback, но это может дать 1048
            try:
                avail_int = int(Decimal(str(avail_raw)).to_integral_value(rounding=ROUND_FLOOR))
            except Exception:
                continue

            total_avail += max(avail_int, 0)

        return total_avail or None

    async def open_long(
        
        self,
        symbol: str,
        qty: float | str,
        *,
        order_type: Literal["Market","Limit"] = "Market",
        price: Optional[str] = None,
        leverage: Optional[int | str] = None,
        client_order_id: Optional[str] = None,
    ):
        """
        Открыть шорт на примерно usdt_amount долларов.
        """
        code = self._to_contract_code(symbol)
        print(await self.usdt_to_qty(symbol=symbol, usdt_amount=100, side="buy"))

        return await self._place_order(
            contract_code=code,
            direction="buy",
            offset="open",
            volume=str(int(float(qty) / float(self.contract_size))),
            order_type=order_type,
            price=price,
            leverage=leverage,
            client_order_id=client_order_id,
        )
    
    async def open_short(
        self,
        symbol: str,
        qty: float | str,
        *,
        order_type: Literal["Market","Limit"] = "Market",
        price: Optional[str] = None,
        leverage: Optional[int | str] = None,
        client_order_id: Optional[str] = None,
    ):
        """
        Открыть шорт на примерно usdt_amount долларов.
        """
        code = self._to_contract_code(symbol)
        print(await self.usdt_to_qty(symbol=symbol, usdt_amount=100, side="buy"))

        return await self._place_order(
            contract_code=code,
            direction="sell",
            offset="open",
            volume=str(int(float(qty) / float(self.contract_size))),
            order_type=order_type,
            price=price,
            leverage=leverage,
            client_order_id=client_order_id,
        )


    # --- high-level: OPEN позиции по USDT-сумме ---
    async def open_long_usdt(
        self,
        symbol: str,
        usdt_amount: float | str,
        *,
        order_type: Literal["Market","Limit"] = "Market",
        price: Optional[str] = None,
        leverage: Optional[int | str] = None,
        client_order_id: Optional[str] = None,
    ):
        """
        Открыть лонг на примерно usdt_amount долларов.
        """
        print(await self.usdt_to_qty(symbol=symbol, usdt_amount=100, side="buy"))
        code = self._to_contract_code(symbol)
        vol  = await self.usdt_to_qty(symbol, usdt_amount, side="buy")

        return await self._place_order(
            contract_code=code,
            direction="buy",
            offset="open",
            volume=vol,
            order_type=order_type,
            price=price,
            leverage=leverage,
            client_order_id=client_order_id,
        )

    async def open_short_usdt(
        self,
        symbol: str,
        usdt_amount: float | str,
        *,
        order_type: Literal["Market","Limit"] = "Market",
        price: Optional[str] = None,
        leverage: Optional[int | str] = None,
        client_order_id: Optional[str] = None,
    ):
        """
        Открыть шорт на примерно usdt_amount долларов.
        """
        code = self._to_contract_code(symbol)
        vol  = await self.usdt_to_qty(symbol, usdt_amount, side="sell")

        return await self._place_order(
            contract_code=code,
            direction="sell",
            offset="open",
            volume=vol,
            order_type=order_type,
            price=price,
            leverage=leverage,
            client_order_id=client_order_id,
        )

    # --- high-level: CLOSE FULL ---
    async def close_long_usdt(
        self,
        symbol: str,
        usdt_amount: float | str,   # игнорируем — всегда закрываем весь доступный лонг
        *,
        order_type: Literal["Market","Limit"] = "Market",
        price: Optional[str] = None,
        client_order_id: Optional[str] = None,
    ):
        """
        Закрыть ВЕСЬ ДОСТУПНЫЙ лонг (direction 'sell', offset 'close').
        """
        code = self._to_contract_code(symbol)
        vol  = await self._get_available_position_volume(symbol, "buy")  # "buy" == лонг у HTX
        if not vol:
            return None

        try:
            return await self._place_order(
                contract_code=code,
                direction="sell",
                offset="close",
                volume=str(vol),
                order_type=order_type,
                price=price,
                leverage=None,
                client_order_id=client_order_id,
            )
        except RuntimeError as e:
            # вернём структуру вместо падения всей программы
            return {
                "error": "close_long_failed",
                "message": str(e),
                "requested_volume": vol,
            }

    async def close_short_usdt(
        self,
        symbol: str,
        usdt_amount: float | str,   # игнорируем — всегда закрываем весь доступный шорт
        *,
        order_type: Literal["Market","Limit"] = "Market",
        price: Optional[str] = None,
        client_order_id: Optional[str] = None,
    ):
        """
        Закрыть ВЕСЬ ДОСТУПНЫЙ шорт (direction 'buy', offset 'close').
        """
        code = self._to_contract_code(symbol)
        vol  = await self._get_available_position_volume(symbol, "sell")  # "sell" == шорт у HTX
        if not vol:
            return None

        try:
            return await self._place_order(
                contract_code=code,
                direction="buy",
                offset="close",
                volume=str(vol),
                order_type=order_type,
                price=price,
                leverage=None,
                client_order_id=client_order_id,
            )
        except RuntimeError as e:
            return {
                "error": "close_short_failed",
                "message": str(e),
                "requested_volume": vol,
            }

    async def close_all_positions(self, symbol: str) -> Dict[str, Any]:
        """
        Пытается закрыть и лонг, и шорт (что доступно).
        Возвращает {"close_long": <resp|None|{error:...}>, "close_short": ...}
        и НЕ бросает RuntimeError наружу.
        """
        results: Dict[str, Any] = {}

        # лонг → закрываем селлом
        await self.usdt_to_qty(symbol=symbol, usdt_amount=100, side="buy")
        long_avail = await self._get_available_position_volume(symbol, "buy")
        print("long avail", long_avail)
        if long_avail:
            try:
                results["long_closed"] = await self._place_order(
                    contract_code=self._to_contract_code(symbol),
                    direction="sell",
                    offset="close",
                    volume=str(long_avail),
                    order_type="Market",
                    leverage=None,
                )
            except RuntimeError as e:
                # Например 1048
                results["long_closed"] = {
                    "error": "1048_or_other",
                    "message": str(e),
                    "requested_volume": long_avail,
                }
        else:
            results["long_closed"] = None

        # шорт → закрываем баем
        short_avail = await self._get_available_position_volume(symbol, "sell")
        if short_avail:
            try:
                results["short_closed"] = await self._place_order(
                    contract_code=self._to_contract_code(symbol),
                    direction="buy",
                    offset="close",
                    volume=str(short_avail),
                    order_type="Market",
                    leverage=None,
                )
            except RuntimeError as e:
                results["short_closed"] = {
                    "error": "1048_or_other",
                    "message": str(e),
                    "requested_volume": short_avail,
                }
        else:
            results["short_closed"] = None

        return results

    # --- positions high-level ---
    @staticmethod
    def _to_iso_ms(ts_ms: int | str | None) -> Optional[str]:
        if ts_ms is None:
            return None
        try:
            val = int(ts_ms)
        except Exception:
            return None
        # эвристика: если это секунды (10 цифр), умножаем до мс
        if val < 10**12:
            val = val * 1000
        try:
            return datetime.fromtimestamp(val / 1000, tz=timezone.utc).isoformat()
        except Exception:
            return None

    async def _all_positions(self) -> List[Dict[str, Any]]:
        """
        Возвращает «сырые» открытые позиции HTX (USDT-маржинальные перпы).
        Пытаемся несколькими эндпоинтами в порядке:
          1) v1 isolated: /linear-swap-api/v1/swap_position_info (без параметров)
          2) v1 isolated: /linear-swap-api/v1/swap_position_info (margin_account=USDT)
          3) v1 cross:    /linear-swap-api/v1/swap_cross_position_info (margin_account=USDT)
          4) v3 unified:  /linear-swap-api/v3/unified_account_position  (если доступно)
        Возвращает список dict'ов позиций (как есть из биржи) или [].
        """
        # 1) v1 isolated — без параметров
        try:
            d = await self._private_post("/linear-swap-api/v1/swap_position_info", body={})
            rows = d.get("data") or []
            if isinstance(rows, list) and rows:
                return rows
        except Exception:
            pass

        # 2) v1 isolated — явно задаём margin_account=USDT
        try:
            d = await self._private_post(
                "/linear-swap-api/v1/swap_position_info",
                body={"margin_account": "USDT"},
            )
            rows = d.get("data") or []
            if isinstance(rows, list) and rows:
                return rows
        except Exception:
            pass

        # 3) v1 cross — если вдруг позиции в кроссе
        try:
            d = await self._private_post(
                "/linear-swap-api/v1/swap_cross_position_info",
                body={"margin_account": "USDT"},
            )
            rows = d.get("data") or []
            if isinstance(rows, list) and rows:
                return rows
        except Exception:
            pass

        # 4) v3 unified — если доступен у аккаунта/регионе
        #    NB: используем GET с подписью в query (как в get_usdt_balance для v3).
        try:
            path = "/linear-swap-api/v3/unified_account_position"
            sp = self._signed_params("GET", path, {})
            r = await self._client.get(path, params=sp)
            r.raise_for_status()
            d = r.json()
            code = str(d.get("code", ""))
            if code in ("0", "200"):
                rows = d.get("data") or []
                # На некоторых аккаунтах data — dict с ключом "positions"
                if isinstance(rows, dict):
                    rows = rows.get("positions") or []
                if isinstance(rows, list) and rows:
                    return rows
        except Exception:
            pass

        return []

    async def get_open_positions(
        self,
        symbol: Optional[str] = None
    ) -> Optional[List[Dict[str, Any]]]:

        """
        Унифицированный список открытых позиций.
        """
        items = await self._all_positions()
        if not len(items):
            return None
        await self.usdt_to_qty(symbol=symbol, usdt_amount=100, side = 'buy')

        out: Dict[str, Any] = {"opened_at": None, 'symbol': None, 'side': None, 'usdt': None, 'leverage': None, 'pnl': None}
        out["opened_at"] = None
        out['symbol'] = symbol
        out['side'] = "short" if items[0]["direction"] == 'sell' else "long"
        out["leverage"] = items[0]["lever_rate"]
        out['entry_usdt'] = float(items[0].get("cost_open")) * float(items[0].get("volume") * self.contract_size /  float(out["leverage"]))
        out['pnl'] = items[0]["profit_unreal"]
        out['entry_price'] = items[0]['cost_open']
        out['market_price'] = items[0]['last_price']
        return out or None

    async def get_usdt_balance(self) -> str:
        """
        Вернёт ТОЛЬКО доступный торговый баланс в USDT (available) как строку.
        Алгоритм:
          1) пробуем v3 GET /linear-swap-api/v3/unified_account_info
          2) если не вышло — v1 POST /linear-swap-api/v1/swap_account_info (margin_account=USDT)
        """
        # ---- helper to call v3/v1 независимо от твоей реализации приватных вызовов ----
        async def _call(method: str, path: str, *, query=None, body=None) -> Dict[str, Any]:
            if hasattr(self, "_private_call"):
                return await self._private_call(method, path, query=query, body=body)
            # fallback: старый приватный пост только для v1
            if method.upper() == "POST":
                return await self._private_post(path, body=body, query=query)
            # v3 требует GET с подписью в query — эмулируем через _signed_params
            sp = self._signed_params("GET", path, query or {})
            r = await self._client.get(path, params=sp)
            r.raise_for_status()
            return r.json()

        # ---- 1) v3 unified_account_info ----
        v3_err = None
        try:
            d3 = await _call("GET", "/linear-swap-api/v3/unified_account_info", query={}, body=None)
            # успешные коды у v3 обычно 0 или 200
            code = str(d3.get("code", ""))
            if code in ("0", "200"):
                info = d3.get("data")
                if isinstance(info, list):
                    picked = None
                    for it in info:
                        cur = (it.get("margin_asset") or it.get("margin_account") or it.get("trade_partition") or "").upper()
                        if "USDT" in cur or cur == "USDT":
                            picked = it
                            break
                    info = picked or (info[0] if info else None)
                if isinstance(info, dict):
                    for k in ("withdraw_available", "available_balance", "margin_available"):
                        if info.get(k) is not None:
                            return format(_d(info[k]), "f")
                raise RuntimeError(f"unified_account_info malformed: {d3}")
            else:
                raise RuntimeError(f"v3 error: {d3}")
        except Exception as e:
            v3_err = e

        # ---- 2) v1 swap_account_info ----
        try:
            d1 = await _call("POST", "/linear-swap-api/v1/swap_account_info", body={"margin_account": "USDT"})
            # v1 success формат: {"status":"ok", "data":[...]}
            if d1.get("status") == "ok":
                arr = d1.get("data") or []
                if not isinstance(arr, list):
                    arr = [arr] if arr else []
                info = None
                for it in arr:
                    cur = (it.get("margin_asset") or it.get("symbol") or it.get("margin_account") or "").upper()
                    if "USDT" in cur or cur == "USDT":
                        info = it
                        break
                info = info or (arr[0] if arr else None)
                if isinstance(info, dict):
                    for k in ("withdraw_available", "margin_available", "available_balance"):
                        if info.get(k) is not None:
                            return format(_d(info[k]), "f")
            raise RuntimeError(f"v1 error or empty: {d1}")
        except Exception as v1_err:
            raise RuntimeError(f"HTX balance error (both v3 and v1 failed): v3={v3_err} | v1={v1_err}")


# ---- simple debug example ----
async def _example():
    symbol = "BIOUSDT"
    async with HTXAsyncClient(HTX_API_KEY, HTX_API_SECRET) as htx:
        # qty = await htx.usdt_to_qty(symbol=symbol, usdt_amount=30, side="sell")
        # print(qty)
        # print("OPEN LONG:", await htx.open_long(symbol = symbol, qty = 10, leverage=5, order_type="Market"))
        # print(await htx.usdt_to_qty(symbol=symbol, usdt_amount=50, side="buy"))
        # print("OPEN SHORT:", await htx.open_long(symbol, 10, leverage=1))
        # await htx.usdt_to_qty(symbol=symbol, usdt_amount=90, side='buy')
        # print("POSITIONS:", await htx.get_open_positions(symbol))
        # print("POSITIONS:", await htx._all_positions())

        # Закрываем обе стороны безопасно (не упадёт по RuntimeError)
        print("CLOSE ALL:", await htx.close_all_positions(symbol))
        print(float(await htx.get_usdt_balance()))


if __name__ == "__main__":
    asyncio.run(_example())
