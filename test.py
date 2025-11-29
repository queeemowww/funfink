import re
symbol = 'LSKUSDT_UMCBLUSDT'
if not len(re.findall(".+USDT", symbol)):
    symbol = symbol+'/USDT'
symbol=symbol.replace('/','')
print(symbol)