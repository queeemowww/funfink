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

import httpx
import ssl
import certifi
from dotenv import load_dotenv
load_dotenv()

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
    """
    Минималистичный async-клиент под USDT-маржинальные perpetual контракты HTX.
    Подпись формата V2 (HmacSHA256 -> base64). Все сети идут через _safe_request
    с автоматическими фолбэками при TLS-ошибках.
    """

    # ----------------------- init / context -----------------------
    def __init__(
        self,
        api_key: str,
        api_secret: str,
        *,
        base_url: str = "https://api.hbdm.com",
        timeout: float = 15.0,
        default_retry_leverage: int = 5,
        # verify:
        #   None -> использовать certifi.where()
        #   True -> системные сертификаты
        #   False -> отключить проверку (НЕБЕЗОПАСНО!)
        #   "path/to/ca-bundle.pem" -> кастомный бандл
        verify: Optional[str | bool] = None,
        # если True, последний фолбэк при SSL-ошибке: verify=False (только для отладки!)
        allow_insecure_ssl: bool = False,
    ):
        if not api_key or not api_secret:
            raise ValueError("HTX_API_KEY / HTX_API_SECRET не заданы")

        # Параметр verify для httpx
        if verify is None:
            verify_param: str | bool = certifi.where()
        else:
            verify_param = verify

        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.api_secret = api_secret.encode("utf-8")
        self._retry_lev = int(default_retry_leverage)
        self._verify_param = verify_param
        self._timeout = timeout
        self._allow_insecure_ssl = allow_insecure_ssl

        # Изначально пробуем http2-клиент
        self._client = httpx.AsyncClient(
            base_url=self.base_url,
            timeout=self._timeout,
            verify=self._verify_param,
            http2=True,
            headers={"Accept": "application/json", "Content-Type": "application/json"},
        )

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.close()

    async def close(self):
        try:
            await self._client.aclose()
        except Exception:
            pass

    # ----------------------- low-level utils -----------------------
    @staticmethod
    def _ts_iso8601() -> str:
        # UTC без миллисекунд (по требованиям подписи HTX)
        return datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")

    @staticmethod
    def _host_from_url(url: str) -> str:
        return urllib.parse.urlparse(url).netloc

    def _host(self) -> str:
        return self._host_from_url(self.base_url)

    @staticmethod
    def _canonical_query(params: Dict[str, Any]) -> str:
        # Сортировка по ключу, urlencode
        items = [(k, "" if v is None else str(v)) for k, v in params.items()]
        items.sort(key=lambda kv: kv[0])
        return urllib.parse.urlencode(items, quote_via=urllib.parse.quote)

    @staticmethod
    def _err_v1(data: Dict[str, Any]) -> str:
        if data.get("status") == "ok":
            return "ok"
        return f"{data.get('err_code')} - {data.get('err_msg')}"

    @staticmethod
    def _err_v3(data: Dict[str, Any]) -> str:
        # v3 обычно {"code":"0","data":{...}} или {"code":"200",...}
        code = data.get("code")
        msg = data.get("msg") or data.get("message") or ""
        return f"{code} - {msg}" if code is not None else f"bad v3: {data}"

    # ----------------------- signing -----------------------
    def _signed_params(
        self,
        method: str,
        path: str,
        extra: Dict[str, Any] | None = None
    ) -> Dict[str, Any]:
        """
        Подпись V2:
        payload = method + '\n' + host + '\n' + path + '\n' + canonical_query
        HmacSHA256(payload) -> base64
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

    # ----------------------- safe networking -----------------------
    async def _recreate_client(self, *, http2: bool, verify: str | bool):
        try:
            await self._client.aclose()
        except Exception:
            pass
        self._client = httpx.AsyncClient(
            base_url=self.base_url,
            timeout=self._timeout,
            verify=verify,
            http2=http2,
            headers={"Accept": "application/json", "Content-Type": "application/json"},
        )

    async def _safe_request(self, method: str, path: str, **kwargs) -> httpx.Response:
        """
        Универсальная обёртка:
          1) Пробует текущую конфигурацию (обычно http2 + verify=certifi)
          2) При SSLCertVerificationError — переключается на HTTP/1.1
          3) Если снова SSL и allow_insecure_ssl=True — последний фолбэк verify=False
        """
        try:
            r = await self._client.request(method, path, **kwargs)
            r.raise_for_status()
            return r
        except httpx.TransportError as e:
            msg = str(e)
            # 1) SSL проблема: пробуем HTTP/1.1 с тем же verify
            if "CERTIFICATE_VERIFY_FAILED" in msg or "certificate verify failed" in msg:
                await self._recreate_client(http2=False, verify=self._verify_param)
                try:
                    r = await self._client.request(method, path, **kwargs)
                    r.raise_for_status()
                    return r
                except httpx.TransportError as e2:
                    msg2 = str(e2)
                    # 2) Последний шанс: verify=False (если разрешено)
                    if (("CERTIFICATE_VERIFY_FAILED" in msg2 or "certificate verify failed" in msg2)
                        and self._allow_insecure_ssl):
                        await self._recreate_client(http2=False, verify=False)
                        r = await self._client.request(method, path, **kwargs)
                        r.raise_for_status()
                        return r
                    raise
            raise

    # ----------------------- private calls (V2-signed) -----------------------
    async def _private_call(
        self,
        method: Literal["GET","POST"],
        path: str,
        *,
        query: Optional[Dict[str, Any]] = None,
        body: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Унифицированный приватный вызов с подписью в query.
        method: GET или POST.
        """
        sp = self._signed_params(method, path, query or {})
        if method == "GET":
            r = await self._safe_request("GET", path, params=sp)
            return r.json()
        else:
            r = await self._safe_request("POST", path, params=sp, json=body or {})
            return r.json()

    async def _private_post(
        self,
        path: str,
        body: Dict[str, Any] | None = None,
        query: Dict[str, Any] | None = None
    ) -> Dict[str, Any]:
        """
        Сохранён для совместимости — делает POST с подписью V2.
        """
        data = await self._private_call("POST", path, query=query, body=body)
        if data.get("status") != "ok":
            raise RuntimeError(f"HTX error: {self._err_v1(data)}")
        return data

    # ----------------------- public helpers -----------------------
    async def _get_contract_info(self, contract_code: str) -> Dict[str, Any]:
        r = await self._safe_request(
            "GET", "/linear-swap-api/v1/swap_contract_info",
            params={"contract_code": contract_code},
        )
        data = r.json()
        if data.get("status") != "ok" or not data.get("data"):
            raise RuntimeError(f"HTX contract_info error: {self._err_v1(data)}")
        return data["data"][0]

    async def _get_ticker(self, contract_code: str) -> Dict[str, Any]:
        r = await self._safe_request(
            "GET", "/linear-swap-ex/market/detail/merged",
            params={"contract_code": contract_code},
        )
        data = r.json()
        if "tick" not in data or not data["tick"]:
            raise RuntimeError("HTX ticker error: empty tick")
        return data["tick"]

    # ----------------------- symbol utils -----------------------
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

    # ----------------------- conversions -----------------------
    async def usdt_to_volume(
        self,
        symbol: str,
        usdt_amount: float | str,
        *,
        side: Literal["buy","sell"],
    ) -> str:
        """
        Преобразует целевую сумму в USDT в объём контрактов (целое).
        contracts = usdt / (price * contract_size)
        Мин. объём учитывается.
        """
        code = self._to_contract_code(symbol)
        tick = await self._get_ticker(code)

        ask = _d(str(tick["ask"][0])) if tick.get("ask") else None
        bid = _d(str(tick["bid"][0])) if tick.get("bid") else None
        price = ask if side == "buy" else bid
        if price is None:
            price = _d(str(tick.get("close") or "0"))
        if price <= 0:
            raise RuntimeError(f"Bad price for {code}: {price}")

        info = await self._get_contract_info(code)
        contract_size = _d(str(info.get("contract_size", "1")))
        min_vol = _d(str(info.get("min_volume") or info.get("min_order_volume") or "1"))
        if contract_size <= 0:
            contract_size = _d(1)

        usdt = _d(usdt_amount)
        contracts = (usdt / (price * contract_size)) if price > 0 else _d(0)

        vol_int = int(contracts.to_integral_value(rounding=ROUND_FLOOR))
        if vol_int < int(min_vol):
            vol_int = int(min_vol)
        return str(vol_int)

    # ----------------------- leverage -----------------------
    async def set_leverage(self, symbol: str, leverage: int | str) -> Dict[str, Any]:
        """
        Устанавливает плечо через /linear-swap-api/v1/swap_switch_lever_rate.
        Требует contract_code и lever_rate.
        """
        code = self._to_contract_code(symbol)
        body = {"contract_code": code, "lever_rate": int(leverage)}
        return await self._private_post("/linear-swap-api/v1/swap_switch_lever_rate", body=body)

    # ----------------------- order placement -----------------------
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
        - Для Market используется order_price_type="opponent".
        - lever_rate обязателен; при 1094 ретраем с self._retry_lev.
        """
        if leverage is not None:
            # Сначала на всякий случай выставим плечо у контракта
            await self.set_leverage(contract_code.replace("-USDT", "USDT"), int(leverage))

        order_price_type = "opponent" if order_type == "Market" else "limit"
        body: Dict[str, Any] = {
            "contract_code": contract_code,
            "direction": direction,
            "offset": offset,
            "volume": int(volume),
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
            data = await self._private_post("/linear-swap-api/v1/swap_order", body=body)
            return data
        except RuntimeError as e:
            msg = str(e)
            need_retry = (" 1094 " in msg) or ("1094-" in msg) or ("1094" in msg and "Leverage" in msg)
            if _retry_on_1094 and need_retry and leverage is None:
                default_lev = self._retry_lev
                await self.set_leverage(contract_code.replace("-USDT", "USDT"), default_lev)
                body_retry = dict(body); body_retry["lever_rate"] = default_lev
                return await self._private_post("/linear-swap-api/v1/swap_order", body=body_retry)
            raise

    # ----------------------- position helpers -----------------------
    async def _get_available_position_volume(
        self,
        symbol: str,
        direction: Literal["buy","sell"],
    ) -> int | None:
        """
        Возвращает доступный к закрытию объём (в КОНТРАКТАХ) по направлению:
          'buy'  -> лонг
          'sell' -> шорт
        Используем поле "available" из swap_position_info (иначе 1048).
        """
        code = self._to_contract_code(symbol)
        data = await self._private_post("/linear-swap-api/v1/swap_position_info", body={"contract_code": code})
        items = data.get("data") or []

        total_avail = 0
        for p in items:
            if (p.get("contract_code") or "").upper() != code:
                continue
            if (p.get("direction") or "").lower() != direction:
                continue
            avail_raw = p.get("available", p.get("available_volume", p.get("available_position")))
            if avail_raw is None:
                avail_raw = p.get("volume")  # fallback (может дать 1048, если не успелла биржа)
            try:
                avail_int = int(Decimal(str(avail_raw)).to_integral_value(rounding=ROUND_FLOOR))
            except Exception:
                continue
            total_avail += max(avail_int, 0)

        return total_avail or None

    # ----------------------- high-level open/close -----------------------
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
        code = self._to_contract_code(symbol)
        vol  = await self.usdt_to_volume(symbol, usdt_amount, side="buy")
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
        code = self._to_contract_code(symbol)
        vol  = await self.usdt_to_volume(symbol, usdt_amount, side="sell")
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

    async def close_long_all(
        self,
        symbol: str,
        *,
        order_type: Literal["Market","Limit"] = "Market",
        price: Optional[str] = None,
        client_order_id: Optional[str] = None,
    ):
        """
        Закрывает ВЕСЬ доступный лонг (direction='sell', offset='close').
        Возвращает None, если лонга нет.
        """
        code = self._to_contract_code(symbol)
        vol  = await self._get_available_position_volume(symbol, "buy")
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
            return {"error": "close_long_failed", "message": str(e), "requested_volume": vol}

    async def close_short_all(
        self,
        symbol: str,
        *,
        order_type: Literal["Market","Limit"] = "Market",
        price: Optional[str] = None,
        client_order_id: Optional[str] = None,
    ):
        """
        Закрывает ВЕСЬ доступный шорт (direction='buy', offset='close').
        Возвращает None, если шорта нет.
        """
        code = self._to_contract_code(symbol)
        vol  = await self._get_available_position_volume(symbol, "sell")
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
            return {"error": "close_short_failed", "message": str(e), "requested_volume": vol}

    async def close_all_positions(self, symbol: str) -> Dict[str, Any]:
        """
        Пытается закрыть и лонг, и шорт (что доступно).
        Не бросает RuntimeError наружу.
        """
        code = self._to_contract_code(symbol)
        results: Dict[str, Any] = {}

        long_avail = await self._get_available_position_volume(symbol, "buy")
        if long_avail:
            try:
                results["long_closed"] = await self._place_order(
                    contract_code=code,
                    direction="sell",
                    offset="close",
                    volume=str(long_avail),
                    order_type="Market",
                    leverage=None,
                )
            except RuntimeError as e:
                results["long_closed"] = {"error": "1048_or_other", "message": str(e), "requested_volume": long_avail}
        else:
            results["long_closed"] = None

        short_avail = await self._get_available_position_volume(symbol, "sell")
        if short_avail:
            try:
                results["short_closed"] = await self._place_order(
                    contract_code=code,
                    direction="buy",
                    offset="close",
                    volume=str(short_avail),
                    order_type="Market",
                    leverage=None,
                )
            except RuntimeError as e:
                results["short_closed"] = {"error": "1048_or_other", "message": str(e), "requested_volume": short_avail}
        else:
            results["short_closed"] = None

        return results

    # ----------------------- positions read -----------------------
    @staticmethod
    def _to_iso_ms(ts_ms: int | str | None) -> Optional[str]:
        if ts_ms is None:
            return None
        try:
            val = int(ts_ms)
        except Exception:
            return None
        # если секунды (10 знаков), умножим до мс
        if val < 10**12:
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
        Возвращает список позиций с примерной оценкой в USDT.
        """
        body: Dict[str, Any] = {}
        if symbol:
            body["contract_code"] = self._to_contract_code(symbol)

        data = await self._private_post("/linear-swap-api/v1/swap_position_info", body=body)
        items = data.get("data") or []
        if not items:
            return None

        resp_ts_ms = data.get("ts")
        out: List[Dict[str, Any]] = []
        for p in items:
            vol_dec = _d(str(p.get("volume", "0")))
            if vol_dec <= 0:
                continue

            code = (p.get("contract_code") or "").upper()
            coin = code.replace("-USDT", "").replace("-USD", "")

            direction = (p.get("direction") or "").lower()  # buy|sell
            pos_type = "long" if direction == "buy" else "short"

            last_price = _d(str(p.get("last_price") or "0"))
            contract_size = _d(str(p.get("contract_size") or "1"))
            position_value = (last_price * contract_size * vol_dec) if last_price > 0 else _d(0)

            opened_raw = (
                p.get("created_at") or p.get("create_time") or p.get("ts") or p.get("open_time") or resp_ts_ms
            )
            opened_iso = self._to_iso_ms(opened_raw)

            out.append({
                "opened_at": opened_iso,
                "symbol": coin,
                "side": pos_type,
                "usdt": format(position_value, "f"),
                "leverage": str(p.get("lever_rate") or ""),
                "pnl": str(p.get("profit_unreal") or "0"),
            })

        return out or None

    # ----------------------- balances -----------------------
    async def get_usdt_balance(self) -> str:
        """
        Возвращает доступный баланс USDT (available) строкой.
        Алгоритм:
          1) v3 GET /linear-swap-api/v3/unified_account_info
          2) при неуспехе — v1 POST /linear-swap-api/v1/swap_account_info (margin_account=USDT)
        """
        # v3
        try:
            d3 = await self._private_call("GET", "/linear-swap-api/v3/unified_account_info", query={}, body=None)
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
                raise RuntimeError(f"v3 error: {self._err_v3(d3)}")
        except Exception:
            # v1 fallback
            d1 = await self._private_post("/linear-swap-api/v1/swap_account_info", body={"margin_account": "USDT"})
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
            raise RuntimeError(f"v1 balance error: {self._err_v1(d1)}")


# ----------------------- simple debug -----------------------
async def _example():
    symbol = "BIOUSDT"
    async with HTXAsyncClient(
        HTX_API_KEY,
        HTX_API_SECRET,
        # verify=None -> certifi, http2 включён, с фолбэками при TLS
        allow_insecure_ssl=False,   # True только для локализации TLS-проблем
    ) as htx:
        bal = await htx.get_usdt_balance()
        print("USDT balance =", bal)
        # print(await htx.open_short_usdt(symbol, 10, leverage=5))
        # print(await htx.close_all_positions(symbol))

if __name__ == "__main__":
    asyncio.run(_example())
