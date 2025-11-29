from __future__ import annotations
import asyncio
import json
from datetime import datetime, UTC
from typing import Any, Dict, List, Optional
from dotenv import load_dotenv
from pandas.errors import EmptyDataError, ParserError
import time
import httpx
import pandas as pd
import requests
import numpy as np
import re
import math
from datetime import datetime, timezone, timedelta
from typing import Dict
from concurrent.futures import ThreadPoolExecutor, as_completed
from api.bitget import BitgetAsyncClient
from api.bybit import BybitAsyncClient
from api.okx import OKXAsyncClient
from api.binance import BinanceAsyncFuturesClient
import warnings
import re
warnings.filterwarnings("ignore", category=UserWarning)
from pairs_parse import Parsing
import os
from pathlib import Path
import sys
import ssl
import certifi
from datetime import datetime, timezone
from typing import Optional, Tuple
import re
import httpx
from decimal import Decimal


load_dotenv() 

CA_BUNDLE = certifi.where()
SSL_CTX = ssl.create_default_context()
SYSTEM_CA = "/etc/ssl/certs/ca-certificates.crt"

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

BINANCE_API_KEY = os.getenv('BINANCE_API_KEY')
BINANCE_API_SECRET = os.getenv('BINANCE_API_SECRET')

TG_TOKEN2 = os.getenv('TG_TOKEN2')
TG_CHAT2 = os.getenv('TG_CHAT2')

class Calc():
    def __init__(self):
        self.leverage = 1
        self.liquidation = 1
        self.dict = {
            "bitget": BitgetAsyncClient(BITGET_API_KEY, BITGET_API_SECRET, BITGET_API_PASSPHRASE),
            "bybit": BybitAsyncClient(BYBIT_API_KEY, BYBIT_API_SECRET, testnet=False),
            "okx": OKXAsyncClient(OKX_API_KEY, OKX_API_SECRET, OKX_API_PASSPHRASE), 
            "binance": BinanceAsyncFuturesClient(BINANCE_API_KEY, BINANCE_API_SECRET)
        }
        if self.leverage == 3:
            self.liquidation = 0.29
        elif self.leverage == 5:
            self.liquidation = 0.18
        elif self.leverage == 10:
            self.liquidation = 0.07

    async def get_funding(self):
        "ВЕРНУТЬ РАЗМЕР ФАНДИНГА"

        return 
    

    async def calc_pnl(self, symbol, long_ex, short_ex):
        if not len(re.findall(".+USDT", symbol)):
            symbol = symbol+'/USDT'
        symbol=symbol.replace('/','')
        long_pos = await self.dict[long_ex].get_open_positions(symbol = symbol)
        short_pos = await self.dict[short_ex].get_open_positions(symbol = symbol)

        return float(long_pos['pnl']) / float(long_pos['usdt']) + float(short_pos['pnl']) / float(short_pos['usdt'])
    
    async def get_open_position(self,symbol,exchange):
        if not len(re.findall(".+USDT", symbol)):
            symbol = symbol+'/USDT'
        symbol=symbol.replace('/','')

        client=self.dict[exchange]
        return await client.get_open_positions(symbol = symbol)


    async def get_qty(self, long_ex, short_ex, sym):
        # 1) расчёт общего размера в USDT — как и раньше
        long_ex_size = math.floor(int(float(await self.dict[long_ex].get_usdt_balance())))
        short_ex_size = math.floor(int(float(await self.dict[short_ex].get_usdt_balance())))
        self.size = min(long_ex_size, short_ex_size) * 0.9

        newsym = sym
        if not len(re.findall(".+USDT", newsym)):
            newsym = newsym + '/USDT'
        newsym = newsym.replace('/','')

        # 2) конвертация USDT -> количество для каждой биржи
        qty_long_raw = await self.dict[long_ex].usdt_to_qty(
            symbol=newsym,
            usdt_amount=self.size,
            side="buy"
        )
        qty_short_raw = await self.dict[short_ex].usdt_to_qty(
            symbol=newsym,
            usdt_amount=self.size,
            side="sell"
        )

        qty_long = Decimal(str(qty_long_raw))
        qty_short = Decimal(str(qty_short_raw))

        # берём минимальный объём в монетах
        qty = min(qty_long, qty_short)

        # 3) приводим qty к шагу контракта тех бирж, где он есть (OKX и др.)
        for ex in (long_ex, short_ex):
            client = self.dict[ex]
            contract_size = getattr(client, "contract_size", None)
            if contract_size is None:
                continue

            cs = Decimal(str(contract_size))
            if cs <= 0:
                continue

            # обрезаем вниз до ближайшего кратного contract_size
            if qty >= cs:
                qty = (qty // cs) * cs

        return float(qty)

    async def open_order(self, direction, symbol, exchange, size):
        if not len(re.findall(".+USDT", symbol)):
            symbol = symbol+'/USDT'
        symbol=symbol.replace('/','')
        print("OPEN SIMBOL, qty = ",symbol, size)
        client = self.dict[exchange]
        if float(size) > 20:
            size = int(float(size))
        if direction=='long':
            await client.open_long(symbol = symbol, qty = str(size * self.leverage), leverage = self.leverage)
        elif direction=='short':
            await client.open_short(symbol = symbol, qty = str(size * self.leverage), leverage = self.leverage)

            
    async def close_order(self, symbol, exchange):
        if not len(re.findall(".+USDT", symbol)):
            symbol = symbol+'/USDT'
        symbol=symbol.replace('/','')
        client = self.dict[exchange]
        res = await client.close_all_positions(symbol = symbol)
        if (res['long_closed'] or res['short_closed']):
            try:
                await self.close_order(symbol=symbol, exchange=exchange)
            except:
                return res
        else:
            return res

class Logic():
    def __init__(self):
        
     # загружаем переменные из .env

    #Подставь свои директории
        self.leave = False
        self.c = Calc()
        self.size = 60
        self.balance = {
            "okx": 0,
            "bitget": 0,
            "bybit": 0,
            "binance": 0
        }
        self.new_balance = 1
        self.all_balance = 1
        self.profit = 0
        self.df_pairs_dir='data/symbols_cleared.csv'
        self.out_csv_dir="temp_data/funding_rates" # куда сохраняем
        self.logs_path ='data/logs.csv'
        self.LOGS_PATH='data/logs.csv'
        self.TG_TOKEN = os.getenv("TG_TOKEN")
        self.TG_CHAT = os.getenv("TG_CHAT")
        self.diff_return=0.15
        #время
        self.check_price_start=10
        self.check_price_finish=59
        self.minutes_for_start_parse = 45
        self.start_pars_pairs=2
        #Интервал парсинга пар в часах
        self.hours_parsingpairs_interval=24
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
        self.demanded_funding_rev=0.5

    async def _position_risk_snapshot(self, exchange: str, symbol: str) -> dict | None:
        """
        Возвращает снимок риска по стороне:
        {
          'exchange': ..., 'symbol': ..., 'side': 'long'|'short',
          'entry_usdt': float, 'pnl': float, 'loss_pct': float, 'qty': float|None
        }
        qty может отсутствовать (зависит от клиента/логов).
        """
        try:
            pos = await self.c.get_open_position(symbol=symbol, exchange=exchange)
            if not pos:
                return None
            entry_usdt = float(pos.get('entry_usdt') or 0.0)
            pnl = float(pos.get('pnl') or 0.0)
            loss_pct = max(0.0, -pnl) / entry_usdt if entry_usdt > 0 else 0.0
            return {
                'exchange': exchange,
                'symbol': symbol,
                'side': pos.get('side'),
                'entry_usdt': entry_usdt,
                'pnl': pnl,
                'loss_pct': loss_pct
            }
        except Exception:
            return None

    async def _anti_liq_guard(self, row: pd.Series):
        """
        row — строка активной позиции из logs_df (status=active).
        Выполняет:
          - оценку риска по обеим сторонам
          - частичную разгрузку при необходимости
          - симметричную подстройку второй стороны
        """
        symbol     = str(row["symbol"])
        long_ex    = str(row["long_exchange"])
        short_ex   = str(row["short_exchange"])

        # 1) Снимки риска
        r_long  = await self._position_risk_snapshot(long_ex,  symbol)
        r_short = await self._position_risk_snapshot(short_ex, symbol)
        if not r_long or not r_short:
            return  # нет данных — аккуратно выходим

        # 2) Пороговые уровни (можно параметризовать через .env)
        DANGER = 0.60     # 60% маржи "съедено" → начинаем снижать
        PANIC  = 0.85     # критика → закрыть почти всё (или всё)

        # 3) Кого резать первым
        hot_side = None
        if r_long['loss_pct'] >= DANGER or r_short['loss_pct'] >= DANGER:
            hot_side = r_long if r_long['loss_pct'] >= r_short['loss_pct'] else r_short

        if not hot_side:
            return  # пока всё ок

        cold_side = r_short if hot_side is r_long else r_long

        # 4) Сколько срезать
        if hot_side['loss_pct'] >= PANIC:
            # Срочно глушим обе стороны
            print(f"[guard] PANIC close {symbol}: {long_ex} & {short_ex}")
            await asyncio.gather(
                self.c.close_order(symbol=symbol, exchange=long_ex),
                self.c.close_order(symbol=symbol, exchange=short_ex),
            )
            # логи
            # тут пометь в logs_df -> status='closed' (как у тебя ниже в run_window)
            return

        # DANGER-режим: аккуратная разгрузка
        # Пропорция разгрузки зависит от перегрева, например 25–50%
        reduce_frac = 0.25 if hot_side['loss_pct'] < 0.75 else 0.5
        await self.c.close_order(exchange=hot_side['exchange'], symbol=symbol)

        # Чтобы не остаться с дельтой, симметрично уменьшаем вторую сторону
        # (можно альтернативно "доворачивать" по USDT-нотионалу, но в простом варианте — одинаковая доля qty)

        await self.c.close_order(exchange=cold_side['exchange'], symbol=symbol)
        print(f"[guard] DANGER on {hot_side['exchange']}:{symbol} loss_pct={hot_side['loss_pct']:.2f} → close ~{reduce_frac*100:.0f}%")
        self.tg_send(f"[guard] DANGER on {hot_side['exchange']}:{symbol} loss_pct={hot_side['loss_pct']:.2f} → close ~{reduce_frac*100:.0f}%")

        # (опционально) Можно сразу обновить logs_df.qty = qty * (1 - reduce_frac)

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
            elif exchange=="okx":
                new_ticker= f"{base}-{quote}-SWAP"
            elif exchange=="binance":                      # 👈 ДОБАВИЛИ
                new_ticker = f"{base}{quote}"              #   BTCUSDT
            return new_ticker
        else:
            quote='USDT'
            if exchange=='bitget':
                new_ticker= f"{ticker}{quote}_UMCBL"
            elif exchange=="bybit":
                new_ticker= f"{ticker}{quote}"
            elif exchange=="okx":
                new_ticker= f"{ticker}-{quote}-SWAP"
            elif exchange=="binance":                      # 👈 ДОБАВИЛИ
                new_ticker = f"{ticker}{quote}"            #   BTCUSDT
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
    def tg_send(self, text: str, spam = None):
        # список "куда слать"
        if not spam:
            destinations = [
                (self.TG_TOKEN,  self.TG_CHAT),
                (TG_TOKEN2, TG_CHAT2),
            ]
        else:
             destinations = [
                (TG_TOKEN2, TG_CHAT2),
             ]
        for token, chat_id in destinations:
            if not token or not chat_id:
                continue  # просто пропускаем, если что-то не настроено

            url = f"https://api.telegram.org/bot{token}/sendMessage"
            data = {"chat_id": chat_id, "text": text}

            try:
                # 1) Пытаемся с нормальной проверкой сертификатов (через certifi)
                requests.post(url, json=data, timeout=10, verify=CA_BUNDLE)

            except requests.exceptions.SSLError as e:
                print("⚠️ SSL ошибка при отправке в Telegram, пробую без verify:", e)
                try:
                    # 2) Fallback: отключаем verify ТОЛЬКО для Телеги
                    requests.post(url, json=data, timeout=10, verify=False)
                except Exception as e2:
                    print("Ошибка отправки в Telegram (fallback тоже упал):", e2)

            except Exception as e:
                print("Ошибка отправки в Telegram:", e)



    def safe_get(self, url: str, *, params=None, timeout: float = 10.0):
        """
        GET-запрос с fallback:
        - сначала verify=CA_BUNDLE
        - при SSL-ошибке пробуем verify=False
        """
        try:
            r = requests.get(url, params=params, timeout=timeout, verify=CA_BUNDLE)
            r.raise_for_status()
            return r
        except requests.exceptions.SSLError as e:
            print(f"⚠️ SSL ошибка при запросе {url} (попробуем без verify): {e}")
            r = requests.get(url, params=params, timeout=timeout, verify=False)
            r.raise_for_status()
            return r

# ---------- 2) парсеры по биржам ----------
    def get_last_price_bitget(self, symbol: str) -> float:
        """
        Bitget v2: GET /api/v2/mix/market/symbol-price
        Возвращаем last price (data[0]["price"]).
        """
        url = "https://api.bitget.com/api/v2/mix/market/symbol-price"

        # symbol может быть вида BTCUSDT_UMCBL — приведём к BTCUSDT + productType
        v2_symbol, product_type = self._bitget_v2_symbol_and_product(symbol)

        for _ in range(5):
            try:
                r = self.safe_get(
                    url,
                    params={"symbol": v2_symbol, "productType": product_type},
                    timeout=10,
                )
                data = r.json()
                d = data.get("data") or []

                if isinstance(d, list) and d:
                    return float(d[0]["price"])
                elif isinstance(d, dict) and "price" in d:
                    return float(d["price"])

                raise ValueError(f"no data for bitget symbol={v2_symbol}: {data}")

            except Exception as e:
                print(f"Ошибка получения цены с bitget ({symbol} → {v2_symbol}): {e}")

        return 100.0


    def get_last_price_bybit(self, symbol: str) -> float:
        url = "https://api.bybit.com/v5/market/tickers"
        for _ in range(5):
            try:
                r = self.safe_get(
                    url,
                    params={"category": "linear", "symbol": symbol},
                    timeout=10,
                )
                data = r.json()
                lst = (data.get("result") or {}).get("list") or []

                if not lst:
                    raise ValueError(f"no data for bybit symbol={symbol}: {data}")

                return float(lst[0]["lastPrice"])
            except Exception as e:
                print(f"Ошибка получения цены с bybit ({symbol}): {e}")
        return 100.0

    def get_last_price_okx(self,symbol: str) -> float:
        flag = 0
        while flag < 5:
            try:
                url = "https://www.okx.com/api/v5/market/ticker"
                r = self.safe_get(url, params={"instId": symbol}, timeout=10)
                r.raise_for_status()
                flag = 5
                return float(r.json()["data"][0]["last"])
            except Exception as e:
                print(f"Ошибка получения цены с okx ({symbol}): {e}")
                flag+=1
                return 100

    def get_last_price_binance(self, symbol: str) -> float:
        """
        Binance USDT-perp futures.
        GET https://fapi.binance.com/fapi/v1/ticker/price?symbol=BTCUSDT
        """
        url = "https://fapi.binance.com/fapi/v1/ticker/price"
        for _ in range(5):
            try:
                r = self.safe_get(url, params={"symbol": symbol}, timeout=10)
                data = r.json()
                price = data.get("price")
                if price is None:
                    raise ValueError(f"no data for binance symbol={symbol}: {data}")
                return float(price)
            except Exception as e:
                print(f"Ошибка получения цены с binance ({symbol}): {e}")
        return 100.0


    def get_futures_last_prices(self,exchange,universal_ticker: str) -> Dict[str, float]:
        symbol = self.convert_symbol_for_exchanges(exchange,universal_ticker)
        if exchange=='bitget':
            price= self.get_last_price_bitget(symbol)
        elif exchange=="bybit":
            price= self.get_last_price_bybit(symbol)
        elif exchange=="okx":
            price=self.get_last_price_okx(symbol)
        elif exchange=="binance":                     # 👈 ДОБАВИЛИ
            price=self.get_last_price_binance(symbol)
        
        return price

    def normalize_symbol(self,sym: str) -> str | None:
            """
            Приводит тикер к виду BASE/QUOTE.
            Понимает форматы:
            BTC-USDT, BTC_USDT, BTCUSDT, BTCUSDT_UMCBL, XBTUSDTM, BTC-USDT-SWAP, AAVEUSDTM и т.п.
            """
            if not isinstance(sym, str) or not sym.strip():
                return None

            s = sym.upper().strip()

            # 1) убрать разделители
            s = s.replace('-', '').replace('_', '').replace(':', '')
            s = re.sub(r'[^A-Z0-9]', '', s)
            # 2) нормализовать "маржинальные" суффиксы к обычным котировкам
            #    USDTM -> USDT, USDCM -> USDC
            for alias, norm in (('USDTM', 'USDT'), ('USDCM', 'USDC')):
                if s.endswith(alias):
                    s = s[:-len(alias)] + norm
                    break

            # 3) убрать общие хвосты типа SWAP/PERP/UMCBL/CMCBL/DMCBL, если остались
            s = re.sub(r'(SWAP|PERP|UMCBL|CMCBL|DMCBL)$', '', s)

            # 4) вычленить базу/котировку по списку известных квот
            for quote in ('USDT', 'USDC', 'USD', 'BTC', 'ETH'):
                if s.endswith(quote) and len(s) > len(quote):
                    base = s[:-len(quote)]
                    return f'{base}/{quote}'

            # fallback — вернуть как есть (уже очищенный)
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
        
    def _bitget_v2_symbol_and_product(self, raw_symbol: str) -> Tuple[str, str]:
        """
        Любой битгетовский тикер (BTCUSDT_UMCBL, BTCUSDT, BTC/USDT и т.п.)
        → (symbol, productType) для v2:
          symbol = 'BTCUSDT'
          productType = 'USDT-FUTURES' / 'USDC-FUTURES' / 'COIN-FUTURES'
        """
        uni = self._normalize_universal(raw_symbol) or raw_symbol.upper()

        if '/' in uni:
            base, quote = uni.split('/')
        else:
            m = re.match(r'^([A-Z0-9]+)(USDT|USDC|USD|BTC|ETH)$', uni)
            if m:
                base, quote = m.group(1), m.group(2)
            else:
                base, quote = uni, 'USDT'

        symbol = f"{base}{quote}"

        if quote == 'USDT':
            product = 'USDT-FUTURES'
        elif quote == 'USDC':
            product = 'USDC-FUTURES'
        else:
            product = 'COIN-FUTURES'

        return symbol, product

    def _normalize_universal(self, symbol: str) -> str:
        """
        Приводим к BASE/QUOTE (например, 'BTCUSDT' -> 'BTC/USDT').
        """
        s = symbol.upper().strip().replace('-', '').replace('_', '')
        for quote in ('USDT', 'USDC', 'USD', 'BTC', 'ETH'):
            if s.endswith(quote) and len(s) > len(quote):
                base = s[:-len(quote)]
                return f'{base}/{quote}'
        return symbol

    def _to_exchange_symbol(self, exchange: str, universal: str) -> str:
        """
        Конвертация универсального символа BASE/QUOTE в формат нужной биржи.
        """
        if '/' in universal:
            base, quote = universal.split('/')
        else:
            # fallback, считаем что без слэша
            # например BTCUSDT -> BTC/USDT
            m = re.match(r'^([A-Z0-9]+)(USDT|USDC|USD|BTC|ETH)$', universal)
            if m:
                base, quote = m.group(1), m.group(2)
            else:
                base, quote = universal, 'USDT'

        ex = exchange.lower()
        if ex == 'bitget':
            return f"{base}{quote}_UMCBL"
        if ex == 'bybit':
            return f"{base}{quote}"
        if ex == 'okx':
            return f"{base}-{quote}-SWAP"
        return f"{base}{quote}"

    async def _get_json(self, client: httpx.AsyncClient, url: str, params: dict | None = None, retries: int = 3) -> dict:
        last_exc = None
        for i in range(retries):
            try:
                r = await client.get(url, params=params, timeout=httpx.Timeout(15.0, connect=15.0, read=15.0))
                r.raise_for_status()
                return r.json()
            except Exception as e:
                last_exc = e
                await asyncio.sleep(0.7 * (i + 1))
        raise last_exc

    def _to_float(self, x) -> Optional[float]:
        try:
            return float(x)
        except Exception:
            return None


    async def get_last_funding(self, exchange: str, symbol: str) -> Optional[Tuple[float, datetime]]:
        """
        Вернёт последнюю ЗАВЕРШЁННУЮ ставку финансирования (previous/last) для пары на указанной бирже.
        Возвращает кортеж (rate, ts_utc), где:
        - rate: float, доля (например 0.0001 == 0.01%)
        - ts_utc: datetime в UTC
        Или None, если не удалось получить данные.
        """
        # нормализуем символ
        if not len(re.findall(".+USDT", symbol)):
            symbol = symbol + '/USDT'
        symbol = symbol.replace('/', '')

        ex = exchange.lower().strip()
        # универсальный символ -> формат биржи
        uni = self._normalize_universal(symbol)
        sym = self._to_exchange_symbol(ex, uni)

        def _ms_to_utc(ts_ms) -> Optional[datetime]:
            try:
                ms = int(ts_ms)
                return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc)
            except Exception:
                return None

        async with httpx.AsyncClient(verify=SSL_CTX) as client:
            try:
                # ---- Bybit ----
                if ex == 'bybit':
                    # GET /v5/market/funding/history?category=linear&symbol=BTCUSDT&limit=1
                    url = "https://api.bybit.com/v5/market/funding/history"
                    params = {"category": "linear", "symbol": sym, "limit": 1}
                    data = await self._get_json(client, url, params)
                    items = (data.get("result") or {}).get("list") or []
                    if items:
                        rate = self._to_float(items[0].get("fundingRate"))
                        ts_utc = _ms_to_utc(items[0].get("fundingRateTimestamp"))
                        if rate is not None and ts_utc is not None:
                            return rate * 100, ts_utc

                    # fallback: inverse
                    params["category"] = "inverse"
                    data = await self._get_json(client, url, params)
                    items = (data.get("result") or {}).get("list") or []
                    if items:
                        rate = self._to_float(items[0].get("fundingRate"))
                        ts_utc = _ms_to_utc(items[0].get("fundingRateTimestamp"))
                        if rate is not None and ts_utc is not None:
                            return rate * 100, ts_utc
                    return None

                # ---- OKX ----
                if ex == 'okx':
                    # GET /api/v5/public/funding-rate-history?instId=BTC-USDT-SWAP&limit=1
                    url = "https://www.okx.com/api/v5/public/funding-rate-history"
                    params = {"instId": sym, "limit": 1}
                    data = await self._get_json(client, url, params)
                    arr = data.get("data") or []
                    if arr:
                        rate = self._to_float(arr[0].get("fundingRate"))
                        ts_utc = _ms_to_utc(arr[0].get("fundingTime"))
                        if rate is not None and ts_utc is not None:
                            return rate * 100, ts_utc
                    return None

                # ---- Bitget ----
                if ex == 'bitget':
                    # v2: GET /api/v2/mix/market/history-fund-rate
                    # возвращает fundingRate в долях и fundingTime в ms
                    v2_symbol, product_type = self._bitget_v2_symbol_and_product(uni)

                    url = "https://api.bitget.com/api/v2/mix/market/history-fund-rate"
                    params = {
                        "symbol": v2_symbol,
                        "productType": product_type,
                        "pageSize": "1",
                    }
                    data = await self._get_json(client, url, params)
                    arr = data.get("data") or []
                    if arr:
                        rate = self._to_float(arr[0].get("fundingRate"))
                        ts_utc = _ms_to_utc(arr[0].get("fundingTime"))
                        if rate is not None and ts_utc is not None:
                            # привожу к %-формату, как у других бирж
                            return rate * 100.0, ts_utc
                    return None
                
                # ---- Binance ----
                if ex == 'binance':
                    # GET /fapi/v1/fundingRate?symbol=BTCUSDT&limit=1
                    url = "https://fapi.binance.com/fapi/v1/fundingRate"
                    params = {"symbol": sym, "limit": 1}
                    data = await self._get_json(client, url, params)
                    # Binance возвращает список
                    if isinstance(data, list) and data:
                        rate = self._to_float(data[0].get("fundingRate"))
                        ts_utc = _ms_to_utc(data[0].get("fundingTime"))
                        if rate is not None and ts_utc is not None:
                            return rate * 100, ts_utc
                    return None

                # неизвестная биржа
                return None

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

    async def fetch_bitget(self, client, row):
        """
        Bitget v2:
          - current funding:  GET /api/v2/mix/market/current-fund-rate
          - next funding time: GET /api/v2/mix/market/funding-time
        funding_time кладём как время следующего фандинга (ms → UTC string).
        """
        s = row["symbol"]
        try:
            v2_symbol, product_type = self._bitget_v2_symbol_and_product(s)

            params = {
                "symbol": v2_symbol,
                "productType": product_type,
            }

            data_fr_task = self.fetch_json(
                client,
                "https://api.bitget.com/api/v2/mix/market/current-fund-rate",
                params,
            )
            data_ft_task = self.fetch_json(
                client,
                "https://api.bitget.com/api/v2/mix/market/funding-time",
                params,
            )

            data_fr, data_ft = await asyncio.gather(data_fr_task, data_ft_task)

            fr = ft = None

            # current funding rate
            if data_fr:
                lst_fr = data_fr.get("data") or []
                if isinstance(lst_fr, list) and lst_fr:
                    fr = lst_fr[0].get("fundingRate")
                elif isinstance(lst_fr, dict):
                    fr = lst_fr.get("fundingRate")

            # next funding time
            if data_ft:
                lst_ft = data_ft.get("data") or []
                if isinstance(lst_ft, list) and lst_ft:
                    ft_ms = lst_ft[0].get("nextFundingTime")
                elif isinstance(lst_ft, dict):
                    ft_ms = lst_ft.get("nextFundingTime")
                else:
                    ft_ms = None

                ft = self.to_dt_ms(ft_ms)

            return {
                "funding_rate": float(fr) if fr is not None else None,  # доля (0.0005)
                "next_funding_rate": None,
                "funding_time": ft,   # время СЛЕДУЮЩЕГО фандинга
                "raw_note": f"symbol_used={v2_symbol}; productType={product_type}",
            }

        except Exception as e:
            return {
                "funding_rate": None,
                "next_funding_rate": None,
                "funding_time": None,
                "raw_note": f"bitget_fail: {e}",
            }

    async def fetch_binance(self, client: httpx.AsyncClient, row: dict) -> dict:
        """
        Binance USDT-perp.
        Используем premiumIndex:
          GET /fapi/v1/premiumIndex?symbol=BTCUSDT
        lastFundingRate — последняя ставка
        nextFundingTime — время следующего фандинга (ms)
        """
        s = row["symbol"]
        try:
            data = await self.fetch_json(
                client,
                "https://fapi.binance.com/fapi/v1/premiumIndex",
                {"symbol": s},
            )

            # API может вернуть dict или список dict'ов
            if isinstance(data, list):
                d = data[0] if data else {}
            else:
                d = data

            fr  = self._to_float(d.get("lastFundingRate"))
            nft = d.get("nextFundingTime")

            return {
                "funding_rate": fr,
                "next_funding_rate": None,
                # как и у Bybit — кладём сюда nextFundingTime,
                # чтобы логика a_in_next/b_in_next работала с "следующим" часом
                "funding_time": self.to_dt_ms(nft),
                "next_funding_time": None,
                "raw_note": None if fr is not None else "no_data",
            }

        except Exception as e:
            return {
                "funding_rate": None,
                "next_funding_rate": None,
                "funding_time": None,
                "next_funding_time": None,
                "raw_note": f"binance_fail: {e}",
            }


    def normalize_exchange_name(self,x: str) -> str:
        s = x.lower()
        # мелкие синонимы на всякий случай
        return s


    async def fetch_one(self,client: httpx.AsyncClient, row: dict) -> dict:
            # ===== Диспетчер =====
        FETCHER_MAP = {
            "bybit": self.fetch_bybit,
            "okx": self.fetch_okx,
            "bitget": self.fetch_bitget,
            "binance": self.fetch_binance
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

        async with httpx.AsyncClient(timeout=self.TIMEOUT, headers=self.HEADERS, verify=SSL_CTX) as client:
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
        """
        Безопасно загружаем логи (если файла нет — создаём пустой DataFrame).
        Если файл повреждён или в него одновременно кто-то пишет — не падаем, а
        начинаем с пустого df (или с обрезанным).
        """
        cols = [
            "ts_utc", "symbol", "long_exchange", "short_exchange",
            "long_funding", "short_funding", "possible_revenue",
            "long_price", "short_price", "diff", "qty", "status"
        ]

        if not os.path.exists(self.LOGS_PATH):
            # файла ещё нет — просто пустой df с нужными колонками
            return pd.DataFrame(columns=cols)

        try:
            # пробуем прочитать как есть
            df = pd.read_csv(self.LOGS_PATH)
        except EmptyDataError:
            # файл есть, но пустой → просто создаём пустой df
            print(f"⚠️ {self.LOGS_PATH} пустой, начинаю с нуля")
            return pd.DataFrame(columns=cols)
        except ParserError as e:
            # файл повреждён (обрезанная строка, битые разделители, гонка записи/чтения и т.д.)
            print(f"⚠️ Ошибка чтения {self.LOGS_PATH}: {e}. Начинаю с пустого лога.")
            return pd.DataFrame(columns=cols)

        # гарантируем наличие всех колонок
        for c in cols:
            if c not in df.columns:
                df[c] = np.nan

        # и порядок колонок
        df = df[cols]

        return df

    def compute_close_threshold_pct(self, possible_rev_frac: float) -> float:
        """
        possible_rev_frac — в долях (0.005 = 0.5%).
        Возвращаем порог в процентных пунктах для current_old_diff (который тоже в %).
        """
        base = 0.20   # базовый порог 0.20 п.п.
        k    = 8.0    # усиление в зависимости от фандинга
        # при 0.5% фандинга → надбавка 0.5 * 8 = 4 п.п.; итог ≈ 4.2 п.п.
        return max(0.10, base + k * possible_rev_frac)


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
            df_pairs1 = pd.read_csv(self.df_pairs_dir)
            df_pairs1["symbol_n"] = df_pairs1["symbol"].apply(self.normalize_symbol)
            df_pairs = df_pairs1[df_pairs1["symbol_n"].duplicated(keep=False)]
            # df_pairs = await asyncio.to_thread(pd.read_csv(df_pairs_dir))
            logs_df=self.load_logs()
            logs_df_c=logs_df.copy()
            if not self.leave:
                logs_df['status']='closed'
            
            df = await self.collect_funding(df_pairs)
            # Сохраняем
            out_csv = self.out_csv_dir + datetime.now(UTC).strftime("%Y%m%d_%H%M") + ".csv"
            df.to_csv(out_csv, index=False, encoding="utf-8")
            df_funding11=df.copy()
            print("Saved:", out_csv)
            df_funding=df
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

            pairs = pairs[pairs['min_exchange'] != pairs['max_exchange']].copy()
            
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
            analytical_df=result_sorted.head(5)
            text=[]
            self.all_balance = 0
            for ex in ['bybit', 'bitget', 'okx', 'binance']:
                res = await self.c.dict[ex].get_usdt_balance()
                self.all_balance += float(res)
                self.balance[ex] = float(res)

            for i in range(5):
                if i == 0:
                    text.append(f"""💰БАЛАНС: {round(self.all_balance, 2)} USDT\n
🟠BYBIT: {self.balance.get('bybit'):.2f}\n
🔵BITGET: {self.balance.get('bitget'):.2f}\n
⚫OKX: {self.balance.get('okx'):.2f}\n
🟡BINANCE: {self.balance.get('binance'):.2f}\n
🔥 Лучшая пара {analytical_df['symbol'].iloc[i]}\n\n""")
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
            df_funding11=df_funding11.dropna(subset=["funding_rate"])
            df_funding1 = df_funding11[['timestamp_utc','exchange','symbol','symbol_n','funding_rate','funding_time']].copy()
            df_funding1['funding_rate'] = df_funding1['funding_rate'] * 100
            df_funding1['funding_rate_abs'] = df_funding1['funding_rate'].abs()
            df_funding1_s=df_funding1.sort_values(by='funding_rate_abs',ascending=False)
            df_funding1_s['funding_time'] = pd.to_datetime(df_funding1_s['funding_time'], utc=True, errors='coerce')

            df_result=result_sorted.copy()
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
                        idx = mask_active.index[e]
                        self.tg_send(f'Оставляем позиции по {symbol} с прошлого часа, несмотря на доход меньше {self.demanded_funding_rev}, они еще не убыточны')
                        logs_df.loc[idx, 'status'] = 'active'
                    else:
                        idx = mask_active.index[e]

                        # 1) баланс до закрытия
                        balance_before = 0.0
                        for ex in ['bybit', 'bitget', 'okx', 'binance']:
                            balance_before += float(await self.c.dict[ex].get_usdt_balance())

                        # 2) закрываем пару
                        await asyncio.gather(
                            self.c.close_order(symbol=symbol, exchange=current_long),
                            self.c.close_order(symbol=symbol, exchange=current_short)
                        )

                        # 3) баланс после закрытия
                        balance_after = 0.0
                        for ex in ['bybit', 'bitget', 'okx', 'binance']:
                            balance_after += float(await self.c.dict[ex].get_usdt_balance())

                        trade_pnl = balance_after - balance_before
                        trade_pnl_pct = (trade_pnl / balance_before * 100) if balance_before > 0 else 0.0

                        # обновляем глобальный "общий баланс", если он тебе нужен
                        self.new_balance = balance_after
                        self.all_balance = balance_after

                        self.profit = trade_pnl_pct / 100  # если хочешь в той же системе, как раньше

                        self.tg_send(
                            f'Закрываем позиции по {symbol} с прошлого часа, доход по фандингу стал отрицательным\n\n'
                            f'💰Баланс до: {balance_before:.2f} USDT\n'
                            f'💰Баланс после: {balance_after:.2f} USDT\n'
                            f'Прибыль по сделке: {trade_pnl:.2f} USDT ({trade_pnl_pct:.2f}%)'
                        )

                        logs_df.loc[idx, 'status'] = 'closed'
                    
                        
            i=0 

            new_symbols=[] 
            while i<=len(df_result)-1 and df_result.iloc[i]['funding_diff_metric']>=self.demanded_funding_rev:
                 
                row = df_result.iloc[i]
                sym = row['symbol']
                print(sym)
                long_ex = row['min_exchange']
                short_ex = row['max_exchange']
                f_long, f_short = self.get_prices_parallel(
                    long_ex,
                    short_ex,
                    sym
                )
                diff_f=(f_long-f_short)/f_short*100

                #если время разное, ищем биржу с лучшим diff
                if df_result.iloc[i]['min_funding_time']==df_result.iloc[i]['max_funding_time']:
                    long_ex = row['min_exchange']
                    short_ex = row['max_exchange']
                    short_funding=row['max_rate']
                    long_funding=row['min_rate']
                    f_long, f_short = self.get_prices_parallel(
                        long_ex,
                        short_ex,
                        sym
                    )
                    diff_f=(f_long-f_short)/f_short*100

                #если время разное, ищем биржу с лучшим diff
                #Отрываем шорт для фандинга, лонг- ищем лучшую биржу по цене
                elif df_result.iloc[i]['min_funding_time']>df_result.iloc[i]['max_funding_time'] and df_result.iloc[i]['max_rate']>0:
                    short_ex=df_result.iloc[i]['max_exchange']
                    long_ex=df_result.iloc[i]['min_exchange']
                    short_funding=row['max_rate']
                    long_funding=row['min_rate']
                    f_long, f_short = self.get_prices_parallel(
                    long_ex,
                    short_ex,
                    df_result.iloc[i]['symbol']
                )
                    diff_f=(f_long-f_short)/f_short*100
                elif df_result.iloc[i]['min_funding_time']>df_result.iloc[i]['max_funding_time'] and df_result.iloc[i]['max_rate']<=0:
                    long_funding=row['max_rate']
                    short_funding=row['min_rate']
                    short_ex=df_result.iloc[i]['min_exchange']
                    long_ex=df_result.iloc[i]['max_exchange']
                    f_long, f_short = self.get_prices_parallel(
                    long_ex,
                    short_ex,
                    df_result.iloc[i]['symbol']
                ) 

                #Отрываем лонг для фандинга, шорт- ищем лучшую биржу по цене   
                elif df_result.iloc[i]['min_funding_time']<df_result.iloc[i]['max_funding_time'] and df_result.iloc[i]['min_rate']>0:
                    long_funding=row['max_rate']
                    short_funding=row['min_rate']
                    short_ex=df_result.iloc[i]['min_exchange']
                    long_ex= df_result.iloc[i]['max_exchange']
                    f_long, f_short = self.get_prices_parallel(
                            long_ex,
                            short_ex,
                            df_result.iloc[i]['symbol']
                        )
                    diff_f=(f_long-f_short)/f_short*100
                elif df_result.iloc[i]['min_funding_time']<df_result.iloc[i]['max_funding_time']and df_result.iloc[i]['min_rate']<=0:
                    short_ex=df_result.iloc[i]['max_exchange']
                    long_ex= df_result.iloc[i]['min_exchange']
                    long_funding=row['min_rate']
                    short_funding=row['max_rate']
                    f_long, f_short = self.get_prices_parallel(
                            long_ex,
                            short_ex,
                            df_result.iloc[i]['symbol']
                        )
                    diff_f=(f_long-f_short)/f_short*100

                if self.pair_already_logged(long_ex, short_ex, logs_df,sym):
                    print(f"Не открываем, ⏭️ биржа из пары уже в используется: {long_ex} ↔ {short_ex}")
                    self.tg_send(f"Не открываем, ⏭️ биржа из пары уже в используется: {long_ex} ↔ {short_ex}")
                    i += 1
                    continue
                #Проверяем каждую биржу и пару, может что то есть в current_possibilities. Тогда что то открывать не надо уже. Проверка доход из current_possibilities>possible_funding-0.5. Тогда используем current_possibilities
                
                mask=logs_df_c[logs_df_c['status']=='active']
                mask_long_eq=mask[(mask['long_exchange']==long_ex)&(mask['symbol']==sym)]
                mask_short_eq=mask[(mask['short_exchange']==short_ex)&(mask['symbol']==sym)]
                if diff_f>df_result.iloc[i]['funding_diff_metric'] and len(mask_long_eq)!=0 and len(mask_short_eq)!=0:
                    new_symbols.append(sym)
                    self.tg_send(f"Разница между биржами {diff_f:.4f} > Дохода от фандинга {df_result.iloc[i]['funding_diff_metric']:.4f}")
                        
                if diff_f>df_result.iloc[i]['funding_diff_metric'] and df_result.iloc[i+1]['funding_diff_metric']<self.demanded_funding_rev and f_long > f_short:
                    mask_active_rest=mask[~mask['symbol'].isin(new_symbols)]
                    self.tg_send(f"Не открываем. Разница {diff_f:.4f} > Доходп от фандинга {df_result.iloc[i]['funding_diff_metric']:.4f}" )
                    for idx, row in mask_active_rest.iterrows():
                        close_rest_sym=row['symbol']
                        close_rest_long=row['long_exchange']
                        close_rest_short=row['short_exchange']
                        print(f'Закрываем то, что осталось и не используется по {close_rest_sym} лонг на {close_rest_long}, шорт на {close_rest_short}')
                        self.tg_send(f'Закрываем то, что осталось и не используется по {close_rest_sym} лонг на {close_rest_long}, шорт на {close_rest_short}')

                        await asyncio.gather(self.c.close_order(symbol=close_rest_sym,exchange=close_rest_long),
                                self.c.close_order(symbol=close_rest_sym, exchange=close_rest_short))
                        logs_df.loc[idx, 'status'] = 'closed'
                        logs_df.to_csv(self.logs_path, index=False)
                    mask_active_syms=mask[mask['symbol'].isin(new_symbols)]
                    for idx, row in mask_active_syms.iterrows():
                        hold_sym=row['symbol']
                        hold_long=row['long_exchange']
                        hold_short=row['short_exchange']
                        print(f'Оставляем позиции по {hold_sym} лонг на {hold_long}, шорт на {hold_short}')
                        self.tg_send(f'Оставляем позиции по {hold_sym} лонг на {hold_long}, шорт на {hold_short}')
                        logs_df.loc[idx, 'status'] = 'active'
                        logs_df.to_csv(self.logs_path, index=False)
                elif diff_f>df_result.iloc[i]['funding_diff_metric']:
                    print(f'Не открываем по {sym}, разница между биржами {diff_f} больше потенциального дохода от фандинга {df_result.iloc[i]["funding_diff_metric"]}')
                    self.tg_send(f'Не открываем по {sym}, разница между биржами {diff_f} больше потенциального дохода от фандинга {df_result.iloc[i]["funding_diff_metric"]}')
                
                else:
                    new_symbols.append(sym)
                            
                    #open_position
                    self.leave = False
                    if len(mask_long_eq)!=0 and len(mask_short_eq)!=0:
                        self.leave = True
                        print(f'Оставляем шорт {short_ex} и лонг {long_ex} по {sym}')
                        self.tg_send(f'Оставляем шорт {short_ex} и лонг {long_ex} по {sym}')
                       

                    elif len(mask_long_eq)!=0:
                        mask_logs_long = (mask['long_exchange'] == long_ex)
                        if mask_logs_long.any():
                            row = mask.loc[mask_logs_long].iloc[0]
                            long_ex_close=row['long_exchange']
                            short_ex_close=row['short_exchange']
                            sym_close=row['symbol']
                            print(f'закрываем позицию по {sym_close}, шорт {short_ex_close}')
                            self.tg_send(f'закрываем позицию по {sym_close}, шорт {short_ex_close}')
                            print(f'Оставляем лонг {long_ex}')
                            print(f'Открываем позицию по {sym}, шорт {short_ex}')
                            self.tg_send(f'Открываем позицию по {sym}, шорт {short_ex}')
                            qty = mask['qty']
                            await self.c.close_order(symbol = sym_close, exchange=short_ex_close)
                            try:
                                await self.c.open_order(direction='short',symbol=sym,exchange=short_ex, size=qty)
                            except Exception as e:
                                print(e)
                                await self.c.close_order(exchange=short_ex_close, symbol=sym_close)
                                await self.c.close_order(exchange=long_ex_close, symbol=sym_close)

                    elif len(mask_short_eq)!=0:
                        mask_logs_short = (mask['short_exchange'] == short_ex)
                        if mask_logs_short.any():
                            row = mask.loc[mask_logs_short].iloc[0]
                            long_ex_close=row['long_exchange']
                            short_ex_close=row['short_exchange']
                            sym_close=row['symbol']
                            print(f'закрываем позицию по {sym_close}, лонг {long_ex_close}')
                            self.tg_send(f'закрываем позицию по {sym_close}, лонг {long_ex_close}')
                            print(f'Оставляем шорт {short_ex}')
                            print(f'Отрываем лонг {long_ex}')
                            self.tg_send(f'Отрываем лонг {long_ex}')
                            qty = mask['qty']
                            await self.c.close_order(symbol = sym_close, exchange=long_ex_close)
                            try:
                                await self.c.open_order(direction='long',symbol=sym,exchange=long_ex, size=qty)
                            except Exception as e:
                                print(e)
                                await self.c.close_order(exchange=short_ex_close, symbol=sym_close)
                                await self.c.close_order(exchange=long_ex_close, symbol=sym_close)

                    #Ищем позицию по с парой нужных нам бирж, закрываем ее.
                    else:
                        if len(mask)!=0:
                            
                            mask_logs = (
                                    (
                                        mask['long_exchange'].isin([long_ex, short_ex]) |
                                        mask['short_exchange'].isin([long_ex, short_ex])
                                    )
                                    & ~mask['symbol'].isin(new_symbols)
                                )

                            if not mask.loc[mask_logs].empty:
                                for _, row in mask.loc[mask_logs].iterrows():
                                    long_ex_close=row['long_exchange']
                                    short_ex_close=row['short_exchange']
                                    sym_close=row['symbol']
                                    print(f'закрываем позицию по {sym_close}, лонг {long_ex_close} , шорт {short_ex_close}')
                                    self.tg_send(f'закрываем позицию по {sym_close}, лонг {long_ex_close} , шорт {short_ex_close}')

                                    await asyncio.gather(self.c.close_order(sym_close,long_ex_close),
                                    self.c.close_order(sym_close,short_ex_close))
                                print(f'Открываем позицию по {sym}, лонг {long_ex} , шорт {short_ex}')
                                self.tg_send(f'Открываем позицию по {sym}, лонг {long_ex} , шорт {short_ex}')
                                qty = await self.c.get_qty(long_ex=long_ex, short_ex=short_ex, sym=sym)
                                try:
                                    await asyncio.gather(
                                    self.c.open_order(direction='long',symbol=sym,exchange=long_ex, size=qty),
                                    self.c.open_order(direction='short',symbol=sym,exchange=short_ex, size=qty))
                                except Exception as e:
                                    print(e)
                                    await self.c.close_order(exchange=long_ex, symbol=sym)
                                    await self.c.close_order(exchange=short_ex, symbol=sym)

                            elif sym in mask['symbol'].values:
                                row = mask.loc[mask['symbol'] == sym].iloc[0]
                                long_ex_close = row['long_exchange']
                                short_ex_close = row['short_exchange']
                                print(f'закрываем позицию по {sym}, лонг {long_ex_close} , шорт {short_ex_close}')
                                self.tg_send(f'закрываем позицию по {sym}, лонг {long_ex_close} , шорт {short_ex_close}')

                                await asyncio.gather(self.c.close_order(symbol=sym,exchange=long_ex_close),
                                    self.c.close_order(symbol=sym,exchange=short_ex_close))
                                print(f'Открываем позицию по {sym}, лонг {long_ex} , шорт {short_ex}')
                                self.tg_send(f'Открываем позицию по {sym}, лонг {long_ex} , шорт {short_ex}')
                                qty = await self.c.get_qty(long_ex=long_ex, short_ex=short_ex, sym=sym)
                                try:
                                    await asyncio.gather(
                                    self.c.open_order(direction='long',symbol=sym,exchange=long_ex, size=qty),
                                    self.c.open_order(direction='short',symbol=sym,exchange=short_ex, size=qty))
                                except Exception as e:
                                    print(e)
                                    await self.c.close_order(exchange=long_ex, symbol=sym)
                                    await self.c.close_order(exchange=short_ex, symbol=sym)
                            else:
                                qty = await self.c.get_qty(long_ex=long_ex, short_ex=short_ex, sym=sym)
                                print("qty = ", qty)
                                print(f'Открываем позицию по {sym}, лонг {long_ex} , шорт {short_ex}, diff_f = {diff_f}')
                                self.tg_send(f'Открываем позицию по {sym}, лонг {long_ex} , шорт {short_ex}, qty = {qty}, diff_f = {diff_f}')
                                try:
                                    await asyncio.gather(
                                    self.c.open_order(direction='long',symbol=sym,exchange=long_ex, size=qty),
                                    self.c.open_order(direction='short',symbol=sym,exchange=short_ex, size=qty))
                                except Exception as e:
                                    print(e)
                                    await self.c.close_order(exchange=long_ex, symbol=sym)
                                    await self.c.close_order(exchange=short_ex, symbol=sym)
                        else:
                            qty = await self.c.get_qty(long_ex=long_ex, short_ex=short_ex, sym=sym)
                            print("qty = ", qty)
                            try:
                                await asyncio.gather(
                                self.c.open_order(direction='long',symbol=sym,exchange=long_ex, size=qty),
                                self.c.open_order(direction='short',symbol=sym,exchange=short_ex, size=qty))
                            except Exception as e:
                                print(e)
                                await self.c.close_order(exchange=long_ex, symbol=sym)
                                await self.c.close_order(exchange=short_ex, symbol=sym)
                            print(f'Открываем позицию по {sym}, лонг {long_ex} , шорт {short_ex}')
                            self.tg_send(f'Открываем позицию по {sym}, лонг {long_ex} , шорт {short_ex}, qty = {qty}')

                    while True:
                        flag = 1
                        while flag <= 3:
                            try:
                                pos_long = await self.c.get_open_position(symbol=sym, exchange=long_ex)
                                pos_short = await self.c.get_open_position(symbol=sym, exchange=short_ex)
                                long_price = float(pos_long['entry_price'])
                                short_price = float(pos_short['entry_price'])
                                break
                            except:
                                flag += 1
                        
                        if long_price and short_price:
                            break
                    if not self.leave:
                        new_row={"ts_utc": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                            "symbol": df_result.iloc[i]['symbol'],
                            "long_exchange": long_ex,
                            "short_exchange":short_ex,
                            "long_funding": long_funding,
                            "short_funding":short_funding,
                            "possible_revenue":df_result.iloc[i]['funding_diff_metric'],
                            "long_price":long_price,
                            "short_price":short_price,
                            'diff':diff_f,
                            'qty': qty,
                            "status":'active'
                            }
                        
                        if df_result.iloc[i+1]['funding_diff_metric']<self.demanded_funding_rev:
                            mask_active_rest=mask[~mask['symbol'].isin(new_symbols)]
                            for idx, row in mask_active_rest.iterrows():
                                close_rest_sym=row['symbol']
                                close_rest_long=row['long_exchange']
                                close_rest_short=row['short_exchange']
                                print(f'Закрываем то, что осталось и не используется по {close_rest_sym} лонг на {close_rest_long}, шорт на {close_rest_short}')
                                await asyncio.gather(self.c.close_order(symbol=close_rest_sym,exchange=close_rest_long),
                                    self.c.close_order(symbol=close_rest_sym, exchange=close_rest_short))
                                logs_df.loc[idx, 'status'] = 'closed'
                        new_row_df=pd.DataFrame([new_row])

                        logs_df = pd.concat([logs_df, pd.DataFrame([new_row])], ignore_index=True)
                        if os.path.exists(self.logs_path):
                            new_row_df.to_csv(self.logs_path, mode="a", header=False, index=False)
                        else:
                            logs_df.to_csv(self.logs_path, index=False)   
                i+=1                   
            print(f"Код занял времени {time_finish-time_start:.2f} секунд")


    async def run_window(self):
        self.confirmations = {}      
        while True:
            now = datetime.now()
            seconds_15 = now.minute
            logs_df=self.load_logs()
            active_logs = logs_df[logs_df['status'] == 'active'].copy()
            if seconds_15 == 1:
                try:
                    # Загружаем свежий CSV и выделяем активные строки
                    logs_df = self.load_logs()

                    # Гарантируем нужные колонки (под твоё требование)
                    for col in ("long_funding", "short_funding"):
                        if col not in logs_df.columns:
                            logs_df[col] = np.nan

                    # Если в файле остались старые имена — обновим и их, чтобы не ломать совместимость
                    legacy_cols = {
                        "long_funding": "long_funding",
                        "short_funding": "short_funding",
                    }
                    for old, new in legacy_cols.items():
                        if old not in logs_df.columns and new in logs_df.columns:
                            logs_df[old] = np.nan  # создадим, чтобы можно было синхронно поддерживать

                    active_idx = logs_df.index[logs_df["status"] == "active"].tolist()
                    if not active_idx:
                        print("Нет активных записей для обновления funding.")
                    else:
                        for idx in active_idx:
                            try:
                                row = logs_df.loc[idx]
                                long_ex  = str(row["long_exchange"])
                                short_ex = str(row["short_exchange"])
                                symbol   = str(row["symbol"])
                                long_last_funding = row["long_funding"]
                                short_last_funding = row["short_funding"]
                                possible_revenue = row["possible_revenue"]
                                short_last_funding, short_last_funding_time  = await self.get_last_funding(exchange=short_ex,  symbol=symbol)
                                long_last_funding, long_last_funding_time  = await self.get_last_funding(exchange=long_ex,  symbol=symbol)

                                if now.hour - long_last_funding_time.hour > 1:
                                    long_last_funding = 0
                                if now.hour - short_last_funding_time.hour > 1:
                                    short_last_funding = 0

                                # Запись в новые поля (как ты просил)
                                logs_df.loc[idx, "long_funding"]  = long_last_funding
                                logs_df.loc[idx, "short_funding"] = short_last_funding
                                
                                if now.hour - datetime.strptime(row['ts_utc'], "%Y-%m-%d %H:%M:%S").hour <= 1:
                                    print("same_hr")
                                    logs_df.loc[idx, "possible_revenue"] = abs(long_last_funding - short_last_funding)
                                else:
                                    print("different _hr", row["possible_revenue"] + abs(long_last_funding - short_last_funding))
                                    logs_df.loc[idx, "possible_revenue"] = row["possible_revenue"] + abs(long_last_funding - short_last_funding)
                            except Exception as e_row:
                                print(f"Ошибка при проверке {logs_df.loc[idx].get('symbol','?')}: {e_row}")

                    try:
                        logs_df.to_csv(self.logs_path, index=False)
                        print(f"✅ funding обновлён в {self.logs_path}")
                    except Exception as e_save:
                        print(f"⚠️ не удалось записать лог: {e_save}")

                except Exception as e:
                    print(f"Неожиданная ошибка обновления funding: {e}")


            # работаем только с 5-й по 45-ю минуту включительно
            if self.check_price_start <= seconds_15 <= self.check_price_finish and not active_logs[active_logs['status']=='active'].empty:
                self.tg_send(spam=True, text=f"🟢 {now.strftime('%H:%M:%S')} — выполняем проверку позиций...")
                print(f"🟢 {now.strftime('%H:%M:%S')} — выполняем проверку позиций...")
                for i in range(len(active_logs)):
                    try:
                        long_ex = active_logs.iloc[i]['long_exchange']
                        short_ex = active_logs.iloc[i]['short_exchange']
                        possible_revenue = active_logs.iloc[i]['possible_revenue']
                        symbol = active_logs.iloc[i]['symbol']
                        print(possible_revenue, "   possible_revenue")
                        self.tg_send(spam=True, text=f"{possible_revenue} - possible revenue")
                        flag = 1
                        while flag <= 3:
                            try:
                                long_pos = await self.c.get_open_position(exchange=long_ex, symbol=symbol)
                                short_pos = await self.c.get_open_position(exchange=short_ex, symbol=symbol)
                                long_price = float(long_pos['market_price'])
                                short_price = float(short_pos['market_price'])
                                break
                            except:
                                flag += 1
                        

                        current_old_diff = ((long_price - active_logs.iloc[i]['long_price']) / active_logs.iloc[i]['long_price'] - (short_price - active_logs.iloc[i]['short_price']) /  active_logs.iloc[i]['short_price']) *100
                        if seconds_15 < 30:
                            self.diff_return = 0.5 - 0.8 * possible_revenue
                        elif 30 <= seconds_15 < self.minutes_for_start_parse:
                            self.diff_return = 0.45 - possible_revenue
                        else:
                            self.diff_return = 0.5

                        print("current long ptice", long_price, "open long price", active_logs.iloc[i]['long_price'])
                        print("current short ptice", short_price,"open short price", active_logs.iloc[i]['short_price'])
                        print(current_old_diff, self.diff_return)
                        self.tg_send(spam=True, text=f"current long price = {long_price}, open long price = {active_logs.iloc[i]['long_price']}\ncurrent short price = {short_price}, open short price = {active_logs.iloc[i]['short_price']}\ncurrent diff = {current_old_diff}, demanded diff return = {self.diff_return}")

                        try:
                            self.confirmations[symbol] = self.confirmations[symbol]
                        except:
                            self.confirmations[symbol] = 0
                        
                        if current_old_diff >= self.diff_return:
                            self.confirmations[symbol] += 1
                        else:
                            self.confirmations[symbol] = 0
                        print(self.confirmations[symbol])
                        self.tg_send(spam=True, text=f"confirmations count = {self.confirmations[symbol]}")
                        if current_old_diff >= self.diff_return and self.confirmations[symbol] >= 5:
                            print(f"⚠️{symbol}: разница выросла ({current_old_diff:.4f} > {self.diff_return:.4f}) — закрываем позиции. Цена закрытия лонг: {long_price}, цена закрытия шорт: {short_price}")
                            self.tg_send(
                                f"⚠️{symbol}: разница выросла ({current_old_diff:.4f} > {self.diff_return:.4f}) — закрываем позиции. "
                                f"Цена закрытия лонг: {long_price}, цена закрытия шорт: {short_price}"
                            )

                            # 1) баланс до закрытия
                            balance_before = 0.0
                            for ex in ['bybit', 'bitget', 'okx', 'binance']:
                                balance_before += float(await self.c.dict[ex].get_usdt_balance())

                            # 2) закрываем пару
                            await asyncio.gather(
                                self.c.close_order(symbol=symbol, exchange=long_ex),
                                self.c.close_order(symbol=symbol, exchange=short_ex)
                            )

                            # 3) баланс после закрытия
                            balance_after = 0.0
                            for ex in ['bybit', 'bitget', 'okx', 'binance']:
                                balance_after += float(await self.c.dict[ex].get_usdt_balance())

                            trade_pnl = balance_after - balance_before
                            trade_pnl_pct = (trade_pnl / balance_before * 100) if balance_before > 0 else 0.0

                            # обновляем "глобальный" баланс
                            self.new_balance = balance_after
                            self.all_balance = balance_after
                            self.profit = trade_pnl_pct / 100

                            self.tg_send(
                                f"💰Баланс до: {balance_before:.2f} USDT\n"
                                f"💰Баланс после: {balance_after:.2f} USDT\n"
                                f"Прибыль по сделке: {trade_pnl:.2f} USDT ({trade_pnl_pct:.2f}%)"
                            )

                            active_logs['status'] = active_logs[active_logs['symbol'] == symbol]['status'] == 'none'
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
                await asyncio.sleep(1)

            else:
                # ждём до следующего часа или следующей 5-й минуты
                print(f"⏸ Сейчас {now.strftime('%H:%M:%S')} — вне окна (ждём 5-ю минуту)")
                self.tg_send(spam= True, text=f"⏸ Сейчас {now.strftime('%H:%M:%S')} — вне окна (ждём 5-ю минуту)")
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



    async def run_daily_task(self):
        while True:
                now = datetime.now()
                target = now.replace(minute=self.start_pars_pairs, second=0, microsecond=0)
                if now >= target:
                    target += timedelta(hours=self.hours_parsingpairs_interval)
                
                wait = (target - now).total_seconds()
                print(f"Ждём {wait/60:.1f} минут до {target.strftime('%H:%M')}")
                await asyncio.sleep(wait)

                # 🔥 запуск задачи
                self.tg_send(f"▶️ Запускаем парсинг новых пар в {datetime.now().strftime('%H:%M:%S')}")
                # await main_process()
                try:
                # await main_process()
                    df_pairs= await Parsing().main()
                    df_pairs.to_csv(self.df_pairs_dir)
                    self.tg_send(f"▶Сохранили новые пары в {datetime.now().strftime('%H:%M:%S')}")
                except:
                    self.tg_send(f"Не получилось сохранить новые пары в {datetime.now().strftime('%H:%M:%S')}")
                    pass
                
                # ждём минуту, чтобы не повторять в том же часу
                await asyncio.sleep(60)

    async def main(self):

        await asyncio.gather(self.run_window(), self.run_at_50(), self.run_daily_task())
        # await asyncio.gather(self.run_daily_task())
        

if __name__ == "__main__":
    asyncio.run(Logic().main())
    