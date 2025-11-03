from __future__ import annotations
import asyncio
import json
from datetime import datetime, UTC
from typing import Any, Dict, List, Optional
from dotenv import load_dotenv
import time
import httpx
import pandas as pd
import requests
import numpy as np
import re
from datetime import datetime, timezone, timedelta
from typing import Dict
from concurrent.futures import ThreadPoolExecutor, as_completed
from api.bitget import BitgetAsyncClient
from api.bybit import BybitAsyncClient
from api.okx import OKXAsyncClient
from api.gate import GateAsyncFuturesClient
from api.htx import HTXAsyncClient
from api.kucoin import KucoinAsyncFuturesClient
import warnings
warnings.filterwarnings("ignore", category=UserWarning)

import os
from pathlib import Path
import sys


OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY")
url = "https://openrouter.ai/api/v1/chat/completions"
load_dotenv() 

BYBIT_API_KEY = os.getenv('BYBIT_API_KEY')
BYBIT_API_SECRET = os.getenv('BYBIT_API_SECRET')

OKX_API_KEY = os.getenv('OKX_API_KEY')
OKX_API_SECRET = os.getenv('OKX_API_SECRET')
OKX_API_PASSPHRASE = os.getenv('OKX_API_PASSPHRASE')

BITGET_API_KEY = os.getenv('BITGET_API_KEY')
BITGET_API_SECRET = os.getenv('BITGET_API_SECRET')
BITGET_API_PASSPHRASE = os.getenv('BITGET_API_PASSPHRASE')

MEXC_API_KEY = os.getenv('MEXC_API_KEY ')
MEXC_API_SECRET = os.getenv('MEXC_API_SECRET')

GATE_API_KEY = os.getenv('GATE_API_KEY')
GATE_API_SECRET = os.getenv('GATE_API_SECRET')

HTX_API_KEY = os.getenv('HTX_API_KEY')
HTX_API_SECRET = os.getenv('HTX_API_SECRET')

EXMO_API_KEY = os.getenv('EXMO_API_KEY')
EXMO_API_SECRET = os.getenv('EXMO_API_SECRET')

KUCOIN_API_KEY = os.getenv('KUCOIN_API_KEY')
KUCOIN_API_SECRET = os.getenv('KUCOIN_API_SECRET')
KUCOIN_API_PASSPHRASE = os.getenv('KUCOIN_API_PASSPHRASE')

class Calc():
    def __init__(self):
        self.size = 100
        self.leverage = 2
        self.dict = {
            "bitget": BitgetAsyncClient(BITGET_API_KEY, BITGET_API_SECRET, BITGET_API_PASSPHRASE),
            "bybit": BybitAsyncClient(BYBIT_API_KEY, BYBIT_API_SECRET, testnet=False),
            "okx": OKXAsyncClient(OKX_API_KEY, OKX_API_SECRET, OKX_API_PASSPHRASE),
            "gate": GateAsyncFuturesClient(GATE_API_KEY, GATE_API_SECRET),
            "htx": HTXAsyncClient(HTX_API_KEY, HTX_API_SECRET),
            "kucoin_futures": KucoinAsyncFuturesClient(KUCOIN_API_KEY, KUCOIN_API_SECRET, KUCOIN_API_PASSPHRASE)
        }


    async def get_funding(self):
        "ВЕРНУТЬ РАЗМЕР ФАНДИНГА"

        return 
    

    async def calc_pnl(self):
        "РАСЧЕТ СУММАРНОГО ПРОФИТА КАЖДУЮ СЕКУНДУ"

        return 
    async def get_open_position(self,symbol,exchange):
        client=self.dict[exchange]
        return await client.get_open_positions(symbol = symbol)


    async def open_order(self, direction, symbol, exchange):
        symbol=symbol.replace('/','')
        client = self.dict[exchange]
        if direction=='long':
            await client.open_long_usdt(symbol = symbol, usdt_amount = self.size, leverage = self.leverage)
        elif direction=='short':
            await client.open_short_usdt(symbol = symbol, usdt_amount = self.size, leverage = self.leverage)
        
    async def close_order(self, symbol, exchange):
        symbol=symbol.replace('/','')
        client = self.dict[exchange]
        await client.close_all_positions(symbol = symbol)

class Logic():
    def __init__(self):
        
     # загружаем переменные из .env

    #Подставь свои директории
        self.df_pairs_dir='data/symbols_cleared.csv'
        self.out_csv_dir="temp_data/funding_rates" # куда сохраняем
        self.logs_path ='data/logs.csv'
        self.LOGS_PATH='data/logs.csv'
        self.TG_TOKEN = os.getenv("TG_TOKEN")
        self.TG_CHAT = os.getenv("TG_CHAT")
        self.diff_return=0.15
        #время
        self.check_price_start=5
        self.check_price_finish=44
        self.minutes_for_start_parse=45
        # ===== Настройки =====
        self.take_risk_size=0.2
        self.TIMEOUT = httpx.Timeout(15.0, connect=15.0, read=15.0)
        self.HEADERS = {
            "User-Agent": "funding-collector/1.0",
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-US,en;q=0.9,ru;q=0.8",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
            "Connection": "keep-alive",
        }
        self.MAX_CONCURRENCY = 20
        self.RETRIES = 3
        self.demanded_funding_rev=0.4
        self.c=Calc()

# ===== Утилиты =====
#self.RETRIES не рботало здесь поставил 3
    async def fetch_json(self,client: httpx.AsyncClient, url: str, params: dict | None = None, retries: int = 3) -> dict:
        RETRIES=self.RETRIES
        last_exc = None
        for i in range(retries):
            try:
                r = await client.get(url, params=params, headers=self.HEADERS)
                r.raise_for_status()
                try:
                    return r.json()
                except json.JSONDecodeError:
                    return {"_raw_text": r.text}
            except Exception as e:
                last_exc = e
                await asyncio.sleep(0.7 * (i + 1))
        raise last_exc


#получение цен
    def convert_symbol_for_exchanges(self,exchange, ticker: str) -> dict:
        if '/' in ticker:
            base, quote = ticker.replace("-", "/").split("/")
            if exchange=='bitget':
                new_ticker= f"{base}{quote}_UMCBL"
            elif exchange=="bybit":
                new_ticker= f"{base}{quote}"
            elif exchange=="gate":
                new_ticker= f"{base}_{quote}"
            elif exchange=="okx":
                new_ticker= f"{base}-{quote}-SWAP"
            elif exchange=="htx":
                new_ticker= f"{base}-{quote}"
            elif exchange=="mexc":
                new_ticker= f"{base}_{quote}"
            elif exchange=="kucoin_futures":
                new_ticker= f"{base}{quote}M"
            return new_ticker
        else:
            quote='USDT'
            if exchange=='bitget':
                new_ticker= f"{ticker}{quote}_UMCBL"
            elif exchange=="bybit":
                new_ticker= f"{ticker}{quote}"
            elif exchange=="gate":
                new_ticker= f"{ticker}_{quote}"
            elif exchange=="okx":
                new_ticker= f"{ticker}-{quote}-SWAP"
            elif exchange=="htx":
                new_ticker= f"{ticker}-{quote}"
            elif exchange=="mexc":
                new_ticker= f"{ticker}_{quote}"
            elif exchange=="kucoin_futures":
                new_ticker= f"{ticker}{quote}M"
            return new_ticker
    #логировангие пары
    def pair_already_logged(self,long_ex, short_ex, logs_df, sym):
                """Проверяем, что пара бирж уже есть в логе (независимо от порядка)."""
                try:
                    if logs_df.empty:
                        return False

                    active_df = logs_df[(logs_df["status"] == "active")]

                    # Проверяем, участвует ли хоть одна из бирж в активной позиции
                    mask = (
                        (active_df["long_exchange"].isin([long_ex, short_ex])) |
                        (active_df["short_exchange"].isin([long_ex, short_ex]))
                    )

                    return not active_df[mask].empty
                except:
                    return False

    #Параллельное получение цен
    def get_prices_parallel(self,min_ex: str, max_ex: str, symbol: str):
        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = {
                executor.submit(self.get_futures_last_prices, min_ex, symbol): "long",
                executor.submit(self.get_futures_last_prices, max_ex, symbol): "short",
            }
            results = {}
            for fut in futures:
                results[futures[fut]] = fut.result()
        return results["long"], results["short"]

#отправка в тг
    def tg_send(self,text: str):
        try:
            url = f"https://api.telegram.org/bot{self.TG_TOKEN}/sendMessage"
            data = {"chat_id": self.TG_CHAT, "text": text}
            requests.post(url, json=data, timeout=10)
        except Exception as e:
            print("Ошибка отправки в Telegram:", e)


# ---------- 2) парсеры по биржам ----------
    def get_last_price_bitget(self,symbol: str) -> float:
        url = "https://api.bitget.com/api/mix/v1/market/ticker"
        try:
            r = requests.get(url, params={"symbol": symbol}, timeout=10)
            r.raise_for_status()
            return float(r.json()["data"]["last"])
        except Exception as e:
            print(f"Ошибка получения цены с bitget ({symbol}): {e}")
            return 100

    def get_last_price_bybit(self,symbol: str) -> float:
        url = "https://api.bybit.com/v5/market/tickers"
        try:
            r = requests.get(url, params={"category": "linear", "symbol": symbol}, timeout=10)
            r.raise_for_status()
            data = r.json()["result"]["list"][0]
            return float(data["lastPrice"])
        except Exception as e:
            print(f"Ошибка получения цены с bybit ({symbol}): {e}")
            return 100

    def get_last_price_gate(self,symbol: str) -> float:
        try:
        # USDT-margined futures
            url = "https://api.gateio.ws/api/v4/futures/usdt/tickers"
            r = requests.get(url, params={"contract": symbol}, timeout=10)
            r.raise_for_status()
            return float(r.json()[0]["last"])
        except Exception as e:
            print(f"Ошибка получения цены с gate ({symbol}): {e}")
            return 100

    def get_last_price_okx(self,symbol: str) -> float:
        try:
            url = "https://www.okx.com/api/v5/market/ticker"
            r = requests.get(url, params={"instId": symbol}, timeout=10)
            r.raise_for_status()
            return float(r.json()["data"][0]["last"])
        except Exception as e:
            print(f"Ошибка получения цены с okx ({symbol}): {e}")
            return 100
    

    def get_last_price_htx(self,symbol: str) -> float:
        try:
            # HTX (Huobi) linear-swap. Берём merged (в нём close = last)
            url = "https://api.hbdm.com/linear-swap-ex/market/detail/merged"
            r = requests.get(url, params={"contract_code": symbol}, timeout=10)
            r.raise_for_status()
            return float(r.json()["tick"]["close"])
        except Exception as e:
            print(f"Ошибка получения цены с htx ({symbol}): {e}")
            return 100

    def get_last_price_mexc(self,symbol: str) -> float:
        # MEXC futures/contract API
        url = "https://contract.mexc.com/api/v1/contract/ticker"
        try:
            r = requests.get(url, params={"symbol": symbol}, timeout=10)
            r.raise_for_status()
            j = r.json()
            
            data = j.get("data")
            if isinstance(data, list) and len(data) > 0:
                data = data[0]
            elif isinstance(data, dict):
                pass  # уже словарь, оставляем
            else:
                return 100 # нет данных
            
            # поле может быть "lastPrice" или "last"
            price_str = data.get("lastPrice") or data.get("last")
            return float(price_str) if price_str else 100

        except Exception as e:
            print(f"Ошибка получения цены с MEXC ({symbol}): {e}")
            return 100

    def get_last_price_kucoin(self,symbol: str) -> float:
        try:
            url = "https://api-futures.kucoin.com/api/v1/ticker"
            r = requests.get(url, params={"symbol": symbol}, timeout=10)
            r.raise_for_status()
            return float(r.json()["data"]["price"])
        except Exception as e:
            print(f"Ошибка получения цены с kucoin ({symbol}): {e}")
            return 100


    def get_futures_last_prices(self,exchange,universal_ticker: str) -> Dict[str, float]:
        symbol = self.convert_symbol_for_exchanges(exchange,universal_ticker)
        if exchange=='bitget':
            price= self.get_last_price_bitget(symbol)
        elif exchange=="bybit":
            price= self.get_last_price_bybit(symbol)
        elif exchange=="gate":
            price= self.get_last_price_gate(symbol)
        elif exchange=="okx":
            price=self.get_last_price_okx(symbol)
        elif exchange=="htx":
            price= self.get_last_price_htx(symbol)
        elif exchange=="mexc":
            price= self.get_last_price_mexc(symbol)
        elif exchange=="kucoin_futures":
            price= self.get_last_price_kucoin(symbol)
        
        return price

    def normalize_symbol(self,sym: str) -> str:
        """
        Приводит тикер к виду BASE/QUOTE.
        Работает с форматами:
        BTC-USDT, BTC_USDT, BTCUSDT, BTCUSDT_UMCBL, XBTUSDTM, BTC-USDT-SWAP и т.п.
        """
        if not isinstance(sym, str) or not sym.strip():
            return None

        s = sym.upper().strip()

        # 1. Удалим лишние суффиксы
        s = re.sub(r'(_UMCBL|_CMCBL|_DMCBL|USDTM|-SWAP|PERP|_PERP)$', '', s)

        # 2. Уберём спецсимволы ($)
        s = re.sub(r'[^A-Z0-9]', '', s)

        # 3. Определим базу и котировку
        # самые частые котировки
        for quote in ["USDT", "USDC", "USD", "BTC", "ETH"]:
            if s.endswith(quote):
                base = s[:-len(quote)]
                return f"{base}/{quote}"

        # fallback — если не распознали
        return s


    def now_utc_iso(self) -> str:
        return datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S%z")


    def ensure_str(self,v: Any) -> Optional[str]:
        return None if v is None else str(v)


    def to_dt_ms(self,ms: Any) -> Optional[str]:
        try:
            return pd.to_datetime(int(ms), unit="ms", utc=True).strftime("%Y-%m-%d %H:%M:%S%z")
        except Exception:
            return None
    
    def to_dt_ms1(self,s: Any) -> Optional[str]:
        try:
            return pd.to_datetime(int(s), unit="s", utc=True).strftime("%Y-%m-%d %H:%M:%S%z")
        except Exception:
            return None


    # ===== Фетчеры по биржам =====
    async def fetch_bybit(self,client: httpx.AsyncClient, row: dict) -> dict:
        """
        Bybit v5. Берём последнюю запись истории как текущий/последний funding.
        category: 'linear' для USDT/USDC, 'inverse' для coin-settle. Если не знаем — пробуем оба.
        """

        symbol = row["symbol"]
        linv = (row.get("linear_inverse") or "").lower()
        categories = (["linear", "inverse"] if linv == "" else [linv])
        fr = None
        last = None
        cat="linear"
        try:
            data = await self.fetch_json(
                client,
                "https://api.bybit.com/v5/market/tickers",
                {"category": cat, "symbol": symbol, "limit": 1},
            )
            items = data.get("result", {}).get("list", [])
            
            if items:
                fr = items[0].get("fundingRate")
                nft = items[0].get("nextFundingTime")
                last = items[0]
                
        except Exception:
            print("Ебануло")
        
        ftime = None
    

        return {
            "funding_rate": fr,
            "next_funding_rate": None,
            "funding_time": self.to_dt_ms(nft),
            "next_funding_time": None,
            "raw_note": None if last else "no_data",
        }

 


    async def fetch_okx(self,client, row):
        s = row["symbol"]
        # нормализация instId: BTC-USDT-SWAP
        if "-" in s and s.endswith("-SWAP"):
            inst_id = s
        elif "-" in s:
            base, quote = s.split("-", 1)
            inst_id = f"{base}-{quote}-SWAP"
        elif s.endswith("USDT"):
            inst_id = f"{s[:-4]}-USDT-SWAP"
        elif s.endswith("USDC"):
            inst_id = f"{s[:-4]}-USDC-SWAP"
        else:
            inst_id = f"{s}-SWAP"

        data = await self.fetch_json(client, "https://www.okx.com/api/v5/public/funding-rate", {"instId": inst_id})
        d = (data.get("data") or [{}])[0]

        def _flt(x):
            try:
                return float(x)
            except Exception:
                return None

        fr  = _flt(d.get("fundingRate"))
        nfr = _flt(d.get("nextFundingRate"))
        fts = d.get("fundingTime")
        nft= d.get("nextFundingTime") # ms

        return {
            "funding_rate": fr,
            "next_funding_rate": nfr,
            "funding_time": self.to_dt_ms(fts),
            "next_funding_time": self.to_dt_ms(nft),
            "raw_note": f"instId={inst_id}",
        }

    async def fetch_bitget(self,client, row):
        s = row["symbol"]
        try:
        
            data_fr_task = self.fetch_json(
                client, "https://api.bitget.com/api/mix/v1/market/current-fundRate", {"symbol": s}
            )
            data_ft_task = self.fetch_json(
                client, "https://api.bitget.com/api/mix/v1/market/funding-time", {"symbol": s}
            )

        
            data_fr, data_ft = await asyncio.gather(data_fr_task, data_ft_task)

            fr = ft = None

            if data_fr:
                lst_fr = data_fr.get("data", {})
                fr = lst_fr.get("fundingRate")

            if data_ft:
                lst_ft = data_ft.get("data", {})
                ft = self.to_dt_ms(lst_ft.get("fundingTime"))

            return {
                "funding_rate": float(fr) if fr is not None else None,
                "next_funding_rate": None,
                "funding_time": ft,
                "raw_note": f"symbol_used={s}; next_settle_time={ft}",
            }

        except Exception as e:
            return {
                "funding_rate": None,
                "next_funding_rate": None,
                "funding_time": None,
                "raw_note": f"bitget_fail: {e}",
            }


    async def fetch_mexc(self,client, row):
        sym = row["symbol"]
        

        url = f"https://contract.mexc.com/api/v1/contract/funding_rate/{sym}"
        last_exc = None

        try:
            data = await self.fetch_json(client, url, {"symbol": sym})
            # иногда ответ: {"data":{"resultList":[...]}} или {"data":[...]}
            if data:
                fr=data.get('data').get("fundingRate")
        
            
                nft = self.to_dt_ms(data.get('data').get("nextSettleTime"))
            return {
                "funding_rate": float(fr) if fr is not None else None,
                "next_funding_rate": None,
                "funding_time": nft,
                "raw_note": data,
            }
        except Exception as e:
            last_exc = e
            

        return {"funding_rate": None, "next_funding_rate": None, "funding_time": None, "raw_note": f"mexc_fail: {last_exc}"}


    async def fetch_kucoin_futures(self,client, row):
        sym = row["symbol"]  # напр., XBTUSDTM
        last_exc = None

        # 1) Попробуем «текущую» (где доступно)
        try:
            cur = await self.fetch_json(client, f"https://api-futures.kucoin.com/api/v1/funding-rate/{sym}/current", {"symbol": sym})
            
            if cur:
                fr=cur.get('data').get("value")
                period=(cur.get('data').get("granularity"))/1000/60/60
                
                nft = self.to_dt_ms(cur.get('data').get("fundingTime"))
                nft1 = self.to_dt_ms(cur.get('data').get("fundingTime")+cur.get('data').get("granularity"))
                return {
                    "funding_rate": float(fr),
                    "next_funding_rate": None,
                    "next_funding_time":nft1,
                    "funding_time": nft,
                    "raw_note": cur,
                }
        except Exception as e:
            last_exc = e
        return {"funding_rate": None, "next_funding_rate": None, "funding_time": None, "raw_note": f"kucoin_fail: {last_exc}"}

    async def fetch_gateio(self,client: httpx.AsyncClient, row: dict) -> dict:
        """
        Gate.io: данные по контракту содержат текущий funding_rate и next_funding_time.
        GET /api/v4/futures/{settle}/contracts/{contract}
        где settle: usdt | btc | eth | gt | ...
        символ контракта: обычно BTC_USDT
        """
        sym = row["symbol"]
        settle = (row.get("settle_asset") or row.get("margin_asset") or "USDT").lower()
        url = f"https://api.gateio.ws/api/v4/futures/{settle}/contracts/{sym}"
        data = await self.fetch_json(client, url)
        d = data if isinstance(data, dict) and "name" in data else data[0] if isinstance(data, list) else data
        fr = d.get("funding_rate")
        nft = d.get("funding_next_apply")
        fi=  d.get("funding_interval")
        return {
            "funding_rate": float(fr) if fr is not None else None,
            "next_funding_rate": None,
            "funding_time": self.to_dt_ms1(nft),
            'next_funding_time': self.to_dt_ms1(nft+fi),
            "raw_note": d,
        }

    async def fetch_htx(self,client: httpx.AsyncClient, row: dict) -> dict:

        sym = row["symbol"]
        
        base_url = "https://api.hbdm.com/linear-swap-api/v1/swap_funding_rate"
        data = await self.fetch_json(client, base_url, {"contract_code": sym})
        if data:
            fr=data.get('data').get('funding_rate')
            ft=self.to_dt_ms(data.get('data').get('funding_time'))
            

        # string "2025-10-07 08:00:00"
        return {
            "funding_rate": float(fr) if fr is not None else None,
            "next_funding_rate": None,
            "funding_time": self.ensure_str(ft),
            "raw_note": data,
        }

    def normalize_exchange_name(self,x: str) -> str:
        s = x.lower()
        # мелкие синонимы на всякий случай
        if s in ("kucoin", "kucoin futures", "kucoin_futures"): s = "kucoin_futures"
        if s in ("huobi", "htx"): s = "htx"
        return s


    async def fetch_one(self,client: httpx.AsyncClient, row: dict) -> dict:
            # ===== Диспетчер =====
        FETCHER_MAP = {
            "bybit": self.fetch_bybit,
            "okx": self.fetch_okx,
            "bitget": self.fetch_bitget,
            "mexc": self.fetch_mexc,
            "kucoin_futures": self.fetch_kucoin_futures,
            "gate": self.fetch_gateio,
            "htx": self.fetch_htx,
        }
        ex = self.normalize_exchange_name(row["exchange"])
        fn = FETCHER_MAP.get(ex)
        base_result = {
            "timestamp_utc": self.now_utc_iso(),
            "exchange": row["exchange"],
            "symbol": row["symbol"],
            "symbol_n":row['symbol_n'],
            "funding_rate": None,
            "next_funding_rate": None,
            "next_funding_time": None,
            "funding_time": None,
            "raw_note": None,
        }
        if not fn:
            base_result["raw_note"] = "unsupported_exchange"
            return base_result
        try:
            res = await fn(client, row)
            base_result.update(res)
        except Exception as e:
            base_result["raw_note"] = f"error: {type(e).__name__}: {e}"
        return base_result


    async def collect_funding(self,df_pairs: pd.DataFrame) -> pd.DataFrame:
        # Нужные колонки
        need_cols = {"exchange", "symbol"}
        missing = need_cols - set(df_pairs.columns)
        if missing:
            raise ValueError(f"В DataFrame отсутствуют колонки: {missing}")

        rows = df_pairs[["exchange", "symbol","symbol_n" ,*([c for c in ("linear_inverse", "settle_asset", "margin_asset") if c in df_pairs.columns])]].to_dict("records")

        async with httpx.AsyncClient(timeout=self.TIMEOUT, headers=self.HEADERS) as client:
            sem = asyncio.Semaphore(self.MAX_CONCURRENCY)
            async def _task(r):
                async with sem:
                    return await self.fetch_one(client, r)
            results = await asyncio.gather(*[_task(r) for r in rows])

        df = pd.DataFrame(results)
        # Приведём типы
        if "funding_rate" in df.columns:
            df["funding_rate"] = pd.to_numeric(df["funding_rate"], errors="coerce")
        if "next_funding_rate" in df.columns:
            df["next_funding_rate"] = pd.to_numeric(df["next_funding_rate"], errors="coerce")
        return df
    
    def load_logs(self):
        """Безопасно загружаем логи (если файла нет — создаём пустой DataFrame)."""
        if os.path.exists(self.LOGS_PATH):
            return pd.read_csv(self.LOGS_PATH)
        else:
            return pd.DataFrame(columns=[
                "ts_utc", "symbol", "long_exchange", "short_exchange",
                "long_funding", "short_funding", "possible_revenue","long_price", "short_price", "diff","status"
            ])


# ===== Пример использования =====
    async def run_at_50(self):
        # 1) если у тебя уже есть CSV с парами:
        while True:
            now = datetime.now()
            
            target = now.replace(minute=self.minutes_for_start_parse, second=15, microsecond=0)
            if now >= target:
                target += timedelta(hours=1)
            await asyncio.sleep((target - now).total_seconds())
            
            print(f"[{datetime.now():%H:%M:%S}] at_50: стартую")
            
            time_start=time.time()
            df_pairs = pd.read_csv(self.df_pairs_dir)
            # df_pairs = await asyncio.to_thread(pd.read_csv(df_pairs_dir))
            logs_df=self.load_logs()
            logs_df_c=logs_df.copy()
            logs_df['status']='closed'
            
            df = await self.collect_funding(df_pairs)
            

            # Сохраняем
            out_csv = self.out_csv_dir + datetime.now(UTC).strftime("%Y%m%d_%H%M") + ".csv"
            df.to_csv(out_csv, index=False, encoding="utf-8")
            df_funding11=df.copy()
            print("Saved:", out_csv)
            df_funding=df
            df_funding = df_funding[df_funding['exchange'] != 'mexc']
            df_funding=df_funding.dropna(subset=['funding_rate'])
            df_funding['symbol_u']=df_funding['symbol']
            df_funding['symbol']=df_funding['symbol_n']

            df = df_funding[['symbol','symbol_u','exchange', 'funding_rate', 'funding_time']].copy()
            df['funding_time'] = pd.to_datetime(df['funding_time'], utc=True)

            # Начинаем формировать лучшие возможности
            pairs = (
                df.merge(df[['symbol','symbol_u','exchange','funding_rate','funding_time']], on='symbol', suffixes=('_a', '_b'))
                .query('exchange_a < exchange_b')  # убираем дубли и самосоединения
                .copy()
            )
            pairs['funding_time_a'] = pd.to_datetime(pairs['funding_time_a'], utc=True)
            pairs['funding_time_b'] = pd.to_datetime(pairs['funding_time_b'], utc=True)
            # 3) окно "следующего часа"
            now = datetime.now(timezone.utc)
            next_hour_end = now + timedelta(hours=1)

            pairs['a_in_next'] = pairs['funding_time_a'].between(now, next_hour_end, inclusive='both')
            pairs['b_in_next'] = pairs['funding_time_b'].between(now, next_hour_end, inclusive='both')


            # оставляем только пары, где хотя бы одно время в следующем часе
            pairs = pairs[(pairs['a_in_next']) | (pairs['b_in_next'])].copy()

            # 4) агрегаты по паре (мин/макс)
            pairs['min_rate'] = pairs[['funding_rate_a', 'funding_rate_b']].min(axis=1)
            pairs['max_rate'] = pairs[['funding_rate_a', 'funding_rate_b']].max(axis=1)

            pairs['min_exchange'] = np.where(
                pairs['funding_rate_a'] <= pairs['funding_rate_b'],
                pairs['exchange_a'], pairs['exchange_b']
            )
            pairs['max_exchange'] = np.where(
                pairs['funding_rate_a'] >= pairs['funding_rate_b'],
                pairs['exchange_a'], pairs['exchange_b']
            )
            pairs['funding_time_a'] = pd.to_datetime(pairs['funding_time_a'], utc=True)
            pairs['funding_time_b'] = pd.to_datetime(pairs['funding_time_b'], utc=True)

            # 2) Выбираем соответствующие времена без astype()
            pairs['min_funding_time'] = pairs['funding_time_a'].where(
                pairs['funding_rate_a'] <= pairs['funding_rate_b'],
                pairs['funding_time_b'],
            )

            pairs['max_funding_time'] = pairs['funding_time_a'].where(
                pairs['funding_rate_a'] >= pairs['funding_rate_b'],
                pairs['funding_time_b'],
            )

            # 5) логика подсчёта funding_diff
            same_time = pairs['funding_time_a'] == pairs['funding_time_b']

            # если времена совпали — обычный спред
            metric_same_time = (pairs['max_rate'] - pairs['min_rate']).abs()

            # если разные — берём ставку той стороны, что в следующем часу
            choice_rate = np.select(
                [
                    pairs['a_in_next'] & ~pairs['b_in_next'],
                    pairs['b_in_next'] & ~pairs['a_in_next'],
                    pairs['a_in_next'] & pairs['b_in_next']  # оба в окне → спред
                ],
                [
                    np.abs(pairs['funding_rate_a']),
                    np.abs(pairs['funding_rate_b']),
                    (pairs['max_rate'] - pairs['min_rate']).abs()
                ],
                default=np.nan
            )

            pairs['funding_diff_metric'] = np.where(same_time, metric_same_time, choice_rate)

            # 6) (опционально) объяснение выбора
            pairs['metric_reason'] = np.where(
                same_time, 'same_time→spread',
                np.select(
                    [
                        pairs['a_in_next'] & ~pairs['b_in_next'],
                        pairs['b_in_next'] & ~pairs['a_in_next'],
                        pairs['a_in_next'] & pairs['b_in_next']
                    ],
                    [
                        'A_in_next→use_rate_a',
                        'B_in_next→use_rate_b',
                        'both_in_next→spread'
                    ],
                    default='no_next_window'
                )
            )

            # 7) выбираем «лучшую» пару на символ (макс. метрика)
            best_pairs = (
                pairs.sort_values(['symbol', 'funding_diff_metric'], ascending=[True, False])
                    .groupby('symbol', as_index=False)
                    .first()
            )
            
            # 8) финальные колонки по вкусу
            result = best_pairs[[

                'symbol',
                'min_rate', 'min_exchange', 'min_funding_time',
                'max_rate', 'max_exchange', 'max_funding_time',
                'funding_diff_metric', 'metric_reason'
            ]].copy()

            # Пример: смотреть топ-20 «лучших» по метрике
            result_sorted=result.sort_values('funding_diff_metric', ascending=False)
            self.res_sorted_dir = 'temp_data/result_sorted'+ datetime.now(UTC).strftime("%Y%m%d_%H%M") + ".csv"
            result_sorted.to_csv(self.res_sorted_dir, index=False, encoding="utf-8")   
            print(result_sorted.head(5))
            headers = {
            "Authorization": f"Bearer {OPENROUTER_API_KEY}",
            "Content-Type": "application/json",
        }
            def analyze(text):
                data = {
                    "model": "openai/gpt-4o-mini",  
                    "messages": [
                        {
                            "role": "user",
                            "content": f"Придумай анекдот про токен с парой {text}. Придумывай злые анекдоты, с пошлостями, чернухой. Криптокуколдизм очень привествуется.",
                        }
                    ],
                }
                # Отправка запроса
                response = requests.post(url, headers=headers, data=json.dumps(data))
                
                # Обработка и вывод ответа
                if response.status_code == 200:
                    result = response.json()
                    try:
                        return(result["choices"][0]["message"]["content"])  # Вывод ответа модели
                    
                    except:
                        return('Сегодня без мема')
                else:
                    return("Сегодня без мема(")
            analytical_df=result_sorted.head(5)
            text=[]
            for i in range(5):
                if i == 0:
                    text.append(f" 🔥 Лучшая пара {analytical_df['symbol'].iloc[i]}\n{analyze(analytical_df['symbol'].iloc[i])}")
                min_time = (analytical_df['min_funding_time'].iloc[i] + timedelta(hours=3)).strftime("%Y-%m-%d %H:%M")

                max_time = (analytical_df['max_funding_time'].iloc[i] + timedelta(hours=3)).strftime("%Y-%m-%d %H:%M")
                text.append(
                    f"Пара {analytical_df['symbol'].iloc[i]}\n"
                    f"— Мин: {analytical_df['min_rate'].iloc[i]*100:.4f}% ({analytical_df['min_exchange'].iloc[i]}) {min_time}\n"
                    f"— Макс: {analytical_df['max_rate'].iloc[i]*100:.4f}% ({analytical_df['max_exchange'].iloc[i]}) {max_time}\n"
                    f"— Потенциальная прибыль: {analytical_df['funding_diff_metric'].iloc[i]*100:.4f}%"
                )
                
            message_text = "\n\n".join(text)
    
            self.tg_send(message_text)
            time_finish=time.time()

            #Функции для бота покупки


            df_funding11["symbol_n"] = df_funding11["symbol"].apply(self.normalize_symbol)
            df_funding11=df_funding11[df_funding11['exchange']!='mexc']
            df_funding11=df_funding11.dropna(subset=["funding_rate"])
            df_funding1=df_funding11[['timestamp_utc','exchange','symbol','symbol_n','funding_rate','funding_time']]
            df_funding1['funding_rate']=df_funding1['funding_rate']*100
            df_funding1['funding_rate_abs']=abs(df_funding1['funding_rate'])
            df_funding1_s=df_funding1.sort_values(by='funding_rate_abs',ascending=False)
            df_funding1_s['funding_time'] = pd.to_datetime(df_funding1_s['funding_time'], utc=True, errors='coerce')

            df_result=result_sorted.copy()
            df_result=df_result[df_result['min_exchange']!='mexc']
            df_result=df_result[df_result['max_exchange']!='mexc']
            df_result['funding_diff_metric']=df_result['funding_diff_metric']*100
            df_result['max_rate']=df_result['max_rate']*100
            df_result['min_rate']=df_result['min_rate']*100
            now = datetime.now(timezone.utc)
            hour_ago = now - timedelta(hours=1)

            #Основная логика
            mask_active=logs_df_c[logs_df_c['status']=='active']

            #Если нету фандингов удовлятворяющих условиям, проверяем, есть ли позиции, которые можно оставить, если не имеют отрицательного фандинга
            print(mask_active)
            if max(df_result['funding_diff_metric'])<self.demanded_funding_rev and len(mask_active)!=0:
                
                for e in range(len(mask_active)):
                    hour_agos = now + timedelta(hours=1)
                    symbol=mask_active.iloc[e]['symbol']
                    current_long=mask_active.iloc[e]['long_exchange']
                    current_short=mask_active.iloc[e]['short_exchange']
                    print(df_funding1_s)
                    df_funding1_filtered = df_funding1_s[
                (df_funding1_s['symbol_n'] == symbol) &
                (df_funding1_s['funding_time'] <= hour_agos)]
                    #подсчет
                    print(df_funding1_filtered)
                    mask = (df_funding1_filtered['symbol_n'] == symbol) & (df_funding1_filtered['exchange'] == current_long)
                    subset = df_funding1_filtered.loc[mask, 'funding_rate']
                    
                    current_long_rev = -subset.iloc[0] if not subset.empty else 0
                    
                    mask = (df_funding1_filtered['symbol_n'] == symbol) & (df_funding1_filtered['exchange'] == current_short)
                    
                    subset = df_funding1_filtered.loc[mask, 'funding_rate']

                    #смотрим ситцацию для текущего часа, где еще можем заработать на фандингах в текузщем часу, ничего не меняя.

                    current_short_rev = subset.iloc[0] if not subset.empty else 0
                    
                    
                    current_total_rev=current_long_rev+current_short_rev
                    print(current_total_rev)
                    if current_total_rev>=0:
                        self.tg_send(f'Оставляем позиции по {symbol} с прошлого часа, несмотря на доход меньше {self.demanded_funding_rev}, они еще не убыточны')
                        logs_df.loc[idx, 'status'] = 'active'
                    else:
                        idx = mask_active.index[e]
                        self.tg_send(f'Закрываем позиции по {symbol} с прошлого часа, доход по фандингу стал отрицательным')
                        await asyncio.gather(self.c.close_order(symbol=symbol, exchange=current_long),
                                self.c.close_order(symbol=symbol, exchange=current_short))
                        # Обновляем значение в исходном df
                        logs_df.loc[idx, 'status'] = 'closed'
            logs_df.to_csv(self.logs_path, index=False)           
                        



            i=0 
            while i<=len(df_result)-1 and df_result.iloc[i]['funding_diff_metric']>self.demanded_funding_rev:
                 
                row = df_result.iloc[i]
                sym = row['symbol']
                print(sym)
                # if df_result.iloc[i]['min_funding_time']==df_result.iloc[i]['max_funding_time']:
                #     print("ЭЛИФ 0", df_result.iloc[i]['min_funding_time'], df_result.iloc[i]['max_funding_time'])

                f_long, f_short = self.get_prices_parallel(
    df_result.iloc[i]['min_exchange'],
    df_result.iloc[i]['max_exchange'],
    df_result.iloc[i]['symbol']
)
                diff_f=(f_long-f_short)/f_long*100
                long_ex = row['min_exchange']
                short_ex = row['max_exchange']

                #если время разное, ищем биржу с лучшим diff
                #Отрываем шорт для фандинга, лонг- ищем лучшую биржу по цене
                # else:
                #     print("СУУКА ЭЛИФ 1", df_result.iloc[i]['min_funding_time'], df_result.iloc[i]['max_funding_time'])
        #             possible_exhanges=df_funding1_s[
        #     (df_funding1_s['symbol_n'] == sym) &
        #     (df_funding1_s['funding_time'] >= hour_ago) &
        #     (df_funding1_s['exchange'] != df_result.iloc[i]['max_exchange'])
        # ]['exchange'].unique().tolist()
                    # print(possible_exhanges)
                    # exchange_list=[]
                    # with ThreadPoolExecutor(max_workers=len(possible_exhanges)) as executor:
                    #     futures = {
                    #         executor.submit(self.get_futures_last_prices, exchange, sym): exchange
                    #         for exchange in possible_exhanges
                    #     }

                    #     for future in as_completed(futures):
                    #         exchange = futures[future]
                    #         try:
                    #             price = future.result()
                    #         except Exception as e:
                    #             print(f"Ошибка получения цены для {exchange}: {e}")
                    #             price = 0
                    #         exchange_list.append({"exchange": exchange, "price": price})
                        
    #                 long_ex = df_result.iloc[i]['min_exchange']
    #                 short_ex=df_result.iloc[i]['max_exchange']
    #                 f_long, f_short = self.get_prices_parallel(
    #     long_ex,
    #     df_result.iloc[i]['max_exchange'],
    #     df_result.iloc[i]['symbol']
    # )
    #                 diff_f=(f_long-f_short)/f_long*100
                    

                #Отрываем лонг для фандинга, шорт- ищем лучшую биржу по цене   
    #             elif df_result.iloc[i]['min_funding_time']<df_result.iloc[i]['max_funding_time']:
    #                 print("#Отрываем лонг для фандинга, шорт- ищем лучшую биржу по цене ЭЛИФ2", df_result.iloc[i]['min_funding_time'], df_result.iloc[i]['max_funding_time'])
    #     #             possible_exhanges=df_funding1_s[
    #     #     (df_funding1_s['symbol_n'] == sym) &
    #     #     (df_funding1_s['funding_time'] >= hour_ago) &
    #     #     (df_funding1_s['exchange'] != df_result.iloc[i]['min_exchange'])
    #     # ]['exchange'].unique().tolist()
    #     #             print(possible_exhanges)
    #     #             exchange_list=[]
    #     #             with ThreadPoolExecutor(max_workers=len(possible_exhanges)) as executor:
    #     #                 futures = {
    #     #                     executor.submit(self.get_futures_last_prices, exchange, sym): exchange
    #     #                     for exchange in possible_exhanges
    #     #                 }
    #     #                 for future in as_completed(futures):
    #     #                     exchange = futures[future]
    #     #                     try:
    #     #                         price = future.result()
    #     #                     except Exception as e:
    #     #                         print(f"Ошибка получения цены для {exchange}: {e}")
    #     #                         price = 0
    #     #                     exchange_list.append({"exchange": exchange, "price": price})
    #                 short_ex = df_result.iloc[i]['max_exchange']
    #                 print(short_ex)
    #                 f_long, f_short = self.get_prices_parallel(
    #     df_result.iloc[i]['min_exchange'],
    #     max(exchange_list, key=lambda x: x["price"])["exchange"],
    #     df_result.iloc[i]['symbol']
    # )
    #                 long_ex= df_result.iloc[i]['min_exchange']
    #                 diff_f=(f_long-f_short)/f_long*100
                    
                
                if self.pair_already_logged(long_ex, short_ex, logs_df,sym):
                    print(f"Не открываем, ⏭️ биржа из пары уже в используется: {long_ex} ↔ {short_ex}")
                    self.tg_send(f"Не открываем, ⏭️ биржа из пары уже в используется: {long_ex} ↔ {short_ex}")
                    i += 1
                    continue
                #Проверяем каждую биржу и пару, может что то есть в current_possibilities. Тогда что то открывать не надо уже. Проверка доход из current_possibilities>possible_funding-0.5. Тогда используем current_possibilities
                

                

                if diff_f>df_result.iloc[i]['funding_diff_metric']:

                    print(f'Не открываем по {sym}, разница между биржами {diff_f} больше потенциального дохода от фандинга {df_result.iloc[i]["funding_diff_metric"]}')
                    self.tg_send(f'Не открываем по {sym}, разница между биржами {diff_f} больше потенциального дохода от фандинга {df_result.iloc[i]["funding_diff_metric"]}')
                else:
                                    
                    
                    #open_position
                    
                    

                    mask=logs_df_c[logs_df_c['status']=='active']
                    mask_long_eq=mask[(mask['long_exchange']==long_ex)&(mask['symbol']==sym)]
                    mask_short_eq=mask[(mask['short_exchange']==short_ex)&(mask['symbol']==sym)]
                    if len(mask_long_eq)!=0 and len(mask_short_eq)!=0:
                        print(f'Оставляем шорт {short_ex} и лонг {long_ex} по {sym}')
                        self.tg_send(f'Оставляем шорт {short_ex} и лонг {long_ex} по {sym}')
                        long_logs='hold'
                        short_logs='hold'
                                    


                    elif len(mask_long_eq)!=0:
                        mask_logs_long = (mask['long_exchange'] == long_ex)
                        if mask_logs_long.any():
                            row = mask.loc[mask_logs_long].iloc[0]
                            short_ex_close=row['short_exchange']
                            sym_close=row['symbol']
                            print(f'закрываем позицию по {sym_close}, шорт {short_ex_close}')
                            self.tg_send(f'закрываем позицию по {sym_close}, шорт {short_ex_close}')
                            print(f'Оставляем лонг {long_ex}')
                            print(f'Открываем позицию по {sym}, шорт {short_ex}')
                            self.tg_send(f'Открываем позицию по {sym}, шорт {short_ex}')
                            
                        
                        self.c.close_order(symbol = mask.iloc[i]['symbol'], exchange=mask.iloc[i]['short_exchange'])
                        self.c.open_order(direction='short',symbol=sym,exchange=short_ex)

                    elif len(mask_short_eq)!=0:
                        mask_logs_short = (mask['short_exchange'] == short_ex)
                        if mask_logs_short.any():
                            row = mask.loc[mask_logs_short].iloc[0]
                            long_ex_close=row['long_exchange']
                            sym_close=row['symbol']
                            print(f'закрываем позицию по {sym_close}, лонг {long_ex_close}')
                            self.tg_send(f'закрываем позицию по {sym_close}, лонг {long_ex_close}')
                            print(f'Оставляем шорт {short_ex}')
                            print(f'Отрываем лонг {long_ex}')
                            self.tg_send(f'Отрываем лонг {long_ex}')

                        short_logs='hold'
                        
                        self.c.close_order(symbol = mask.iloc[i]['symbol'], exchange=mask.iloc[i]['long_exchange'])
                        self.c.open_order(direction='long',symbol=sym,exchange=long_ex)
                    #Ищем позицию по с парой нужных нам бирж, закрываем ее.
                    else:
                        if len(mask)!=0:
                            
                            mask_logs = (mask['long_exchange'] == long_ex) | (mask['short_exchange'] == short_ex)
                            
                            if mask_logs.any():
                                row = mask.loc[mask_logs].iloc[0]
                                long_ex_close=row['long_exchange']
                                short_ex_close=row['short_exchange']
                                sym_close=row['symbol']
                                print(f'закрываем позицию по {sym_close}, лонг {long_ex_close} , шорт {short_ex_close}')
                                print(f'Открываем позицию по {sym}, лонг {long_ex} , шорт {short_ex}')
                                self.tg_send(f'закрываем позицию по {sym_close}, лонг {long_ex_close} , шорт {short_ex_close}')
                                self.tg_send(f'Открываем позицию по {sym}, лонг {long_ex} , шорт {short_ex}')
                                
                                await asyncio.gather(self.c.close_order(symbol=sym_close,exchange=long_ex_close),
                                self.c.close_order(symbol=sym_close, exchange=short_ex_close))
                                await asyncio.gather(
                                self.c.open_order(direction='long',symbol=sym,exchange=long_ex),
                                self.c.open_order(direction='short',symbol=sym,exchange=short_ex))
                            else:
                                self.tg_send(f'Открываем позицию по {sym}, лонг {long_ex} , шорт {short_ex}')
                                print(f'Открываем позицию по {sym}, лонг {long_ex} , шорт {short_ex}')
                                await asyncio.gather(
                                self.c.open_order(direction='long',symbol=sym,exchange=long_ex),
                                self.c.open_order(direction='short',symbol=sym,exchange=short_ex))
                                print(3)
                                
                        else:
                            await asyncio.gather(
                                self.c.open_order(direction='long',symbol=sym,exchange=long_ex),
                                self.c.open_order(direction='short',symbol=sym,exchange=short_ex))
                            
                            
                            print(f'Открываем позицию по {sym}, лонг {long_ex} , шорт {short_ex}')
                            self.tg_send(f'Открываем позицию по {sym}, лонг {long_ex} , шорт {short_ex}')

                    new_row={"ts_utc": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                        "symbol": df_result.iloc[i]['symbol'],
                        "long_exchange": long_ex,
                        "short_exchange":short_ex,
                        "long_funding": df_result.iloc[i]['min_rate'],
                        "short_funding":df_result.iloc[i]['max_rate'],
                        "possible_revenue":df_result.iloc[i]['funding_diff_metric'],
                        "long_price":f_long,
                        "short_price":f_short,
                        'diff':diff_f,
                        "status":'active'
                        }
                    new_row_df=pd.DataFrame([new_row])

                    logs_df = pd.concat([logs_df, pd.DataFrame([new_row])], ignore_index=True)
                    if os.path.exists(self.logs_path):
                        new_row_df.to_csv(self.logs_path, mode="a", header=False, index=False)
                    else:
                        logs_df.to_csv(self.logs_path, index=False)
                            
                i+=1
                    
            

            print(f"Код занял времени {time_finish-time_start:.2f} секунд")


    async def run_window(self):       
        while True:
            now = datetime.now()
            minute = now.minute
            logs_df=self.load_logs()
            active_logs = logs_df[logs_df['status'] == 'active'].copy()
            
            # работаем только с 5-й по 45-ю минуту включительно
            if self.check_price_start <= minute <= self.check_price_finish and not active_logs[active_logs['status']=='active'].empty:
                print(f"🟢 {now.strftime('%H:%M')} — выполняем проверку позиций...")
                
                for i in range(len(active_logs)):
                    try:
                        long_ex = active_logs.iloc[i]['long_exchange']
                        print(active_logs)
                        short_ex = active_logs.iloc[i]['short_exchange']
                        
                        symbol = active_logs.iloc[i]['symbol']
                        print(long_ex,symbol)
                        long_price = self.get_futures_last_prices(long_ex, symbol)
                        
                        short_price = self.get_futures_last_prices(short_ex, symbol)
                        long_price, short_price = self.get_prices_parallel(
        long_ex,
        short_ex,
        symbol
    )
                        old_diff = (active_logs.iloc[i]['long_price']-active_logs.iloc[i]['short_price'])/long_price*100
                        current_diff = (long_price - short_price)/long_price*100
                        if current_diff > old_diff+self.diff_return:
                            print(f"⚠️ {symbol}: разница выросла ({current_diff:.4f} > {old_diff:.4f}) — закрываем позиции")
                            self.tg_send(f"⚠️ {symbol}: разница выросла ({current_diff:.4f} > {old_diff:.4f}) — закрываем позиции")
                            await asyncio.gather(
                            self.c.close_order(symbol=symbol, exchange=long_ex),
                            self.c.close_order(symbol=symbol, exchange=short_ex)
                        )
                            active_logs['status']=active_logs[active_logs['symbol']==symbol]['status']=='none'
                            # close_positions(long_ex, short_ex, symbol)
                            mask_close = (
                                (logs_df['symbol'] == symbol) &
                                (logs_df['long_exchange'] == long_ex) &
                                (logs_df['short_exchange'] == short_ex) &
                                (logs_df['status'] == 'active')
                            )
                            logs_df.loc[mask_close, 'status'] = 'closed'
                            try:
                                logs_df.to_csv(self.logs_path, index=False)
                            except Exception as e:
                                print(f"⚠️ не удалось записать лог: {e}")

                            

                    except Exception as e:
                        print(f"Ошибка при проверке {active_logs.iloc[i]['symbol']}: {e}")

                # проверяем каждые 2 минуты, пока идёт окно
                await asyncio.sleep(10)

            else:
                # ждём до следующего часа или следующей 5-й минуты
                print(f"⏸ Сейчас {now.strftime('%H:%M')} — вне окна (ждём 5-ю минуту)")
                await asyncio.sleep(60)


            "УДАЛЕНИЕ ВРЕМЕННЫХ НЕНУЖНЫХ ФАЙЛОВ"
            base = Path('temp_data')
            files = [p for p in base.iterdir() if p.is_file()]
            if len(files) >= 8:
                files_sorted = sorted(files, key=lambda p: p.stat().st_mtime)
                # Берём ДВА самых старых
                to_delete = files_sorted[:2]

                for f in to_delete:
                    try:
                        os.remove(f)
                        print(f"[OK] Удалён: {f.name}")
                    except Exception as e:
                        print(f"[ERR] Не удалось удалить {f.name}: {e}")

    async def main(self):
        await asyncio.gather(self.run_window(), self.run_at_50())

if __name__ == "__main__":

    asyncio.run(Logic().main())

