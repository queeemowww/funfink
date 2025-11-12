import re
import math
import pandas as pd

# df = pd.read_csv("temp_data/result_sorted20251106_1605.csv")
# print(df)

# s1 = 65.84

# s2 = 64.20
# print(int(int(min(s1,s2)) * 0.95))

print(re.findall(r'(?i)(?<![A-Za-z])(long|short)(?![A-Za-z])', '__dual__lfg__')[0])
