import pandas as pd

df = pd.read_csv(r"C:\Q-TRON-32\backtest\data\20220103.csv")
print(df['code'].head(10))