from api.bitget import BitgetAsyncClient
from api.bybit import BybitAsyncClient
from api.okx import OKXAsyncClient
from api.gate import GateAsyncFuturesClient
from api.htx import HTXAsyncClient
from api.kucoin import KucoinAsyncFuturesClient
from funding_rate_positions import Logic
import os
from dotenv import load_dotenv
import asyncio


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
        self.dict = {
            "bitget": BitgetAsyncClient(BITGET_API_KEY, BITGET_API_SECRET, BITGET_API_PASSPHRASE),
            "bybit": BybitAsyncClient(BYBIT_API_KEY, BYBIT_API_SECRET, testnet=False),
            "okx": OKXAsyncClient(OKX_API_KEY, OKX_API_SECRET, OKX_API_PASSPHRASE),
            "gate": GateAsyncFuturesClient(GATE_API_KEY, GATE_API_SECRET),
            "htx": HTXAsyncClient(HTX_API_KEY, HTX_API_SECRET),
            "kucoin": KucoinAsyncFuturesClient(KUCOIN_API_KEY, KUCOIN_API_SECRET, KUCOIN_API_PASSPHRASE)
        }


    async def get_funding(self):
        "ВЕРНУТЬ РАЗМЕР ФАНДИНГА"

        return 
    

    async def calc_pnl(self):
        "РАСЧЕТ СУММАРНОГО ПРОФИТА КАЖДУЮ СЕКУНДУ"

        return 

    async def open_order(self, symbol, exchange, side, size, levarage):
        size = 10
        leavarage = 3

        client = self.dict[exchange]
        if side == "long":
            await client.open_long_usdt(symbol, 20, leverage=5)
        # print(await client.get_open_positions(symbol = symbol))
        # await client.close_all_positions(symbol = symbol)
        # while await self.calc_pnl() < 0.05:
        #     print()
    
    async def close_order(self, symbol, exchange):
        client = self.dict[exchange]
        await client.close_all_positions(symbol = symbol)
        

if __name__ == "__main__":
    c = Calc()
    # asyncio.run(c.open_order("BIOUSDT", "bybit", 20, 5))
    print(asyncio.run(c.close_order("BIOUSDT", "bybit")))