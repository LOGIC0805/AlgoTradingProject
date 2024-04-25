import pandas as pd
import numpy as np
import warnings

warnings.filterwarnings("ignore")

ic_data_df = pd.read_csv("./data/IC.csv")
contract_info_df = pd.read_csv("./data/contract_info.csv")

contract_info_df = contract_info_df[contract_info_df["product"]=="IC"]
ic_dominant_df = pd.merge(ic_data_df, contract_info_df, how="inner", on=["TRADE_DAYS", "instrument"])
ic_dominant_df = ic_dominant_df.sort_values("date")
ic_dominant_df.index = range(len(ic_dominant_df.index))
minbar_data = ic_dominant_df.set_index("date")
minbar_data.index = pd.to_datetime(minbar_data.index)
data_30min = minbar_data.resample("30min").agg({
    "open":"first",
    "close":"last",
    "high":"max",
    "low":"min",
    "volume":"sum",
    "money":"sum",
    "TRADE_DAYS": "last"
}).dropna()
data_30min.to_parquet("./data_30min.parquet")

bbands_up = data_30min["close"].rolling(20).mean() + 2* data_30min["close"].rolling(20).std()
bbands_down = data_30min["close"].rolling(20).mean() - 2* data_30min["close"].rolling(20).std()
ma_20 = data_30min["close"].rolling(20).mean()
ma_5 = data_30min["close"].rolling(5).mean()
atr = pd.concat([
    data_30min["high"] - data_30min["low"], data_30min["high"] - data_30min["low"].shift(1),
    data_30min["close"].shift(1) - data_30min["low"]
], axis=1).max(axis=1)
dm_positive = (data_30min["high"]>data_30min["high"].shift(1)) \
            * (data_30min["low"]>data_30min["low"].shift(1)) \
            * (data_30min["high"]-data_30min["high"].shift(1))

dm_negetive = (data_30min["high"]<data_30min["high"].shift(1)) \
            * (data_30min["low"]<data_30min["low"].shift(1)) \
            * (data_30min["low"].shift(1) - data_30min["low"])
dm_positive_14 = dm_positive.rolling(14).mean() / atr.rolling(14).mean()
dm_negetive_14 = dm_negetive.rolling(14).mean() / atr.rolling(14).mean()
adx = (abs(dm_positive_14 - dm_negetive_14) / abs(dm_positive_14 + dm_negetive_14) *100).rolling(14).mean()

aroon_up = (24 - data_30min["high"].rolling(25).apply(lambda x: pd.Series(x).argmax())) / 25
aroon_down = (24 - data_30min["high"].rolling(25).apply(lambda x: pd.Series(x).argmin())) / 25
aroon_osc = (aroon_up - aroon_down) * 100

rsi = (data_30min["close"].pct_change() * (data_30min["close"].pct_change()>0)).rolling(24).sum() \
        / abs(data_30min["close"].pct_change()).rolling(24).sum() * 100

typical_price = (data_30min["close"] + data_30min["high"] + data_30min["low"])
mf_positive = (data_30min["volume"] * typical_price * (typical_price>typical_price.shift(1))).rolling(14).mean()
mf_negative = (data_30min["volume"] * typical_price * (typical_price<typical_price.shift(1))).rolling(14).mean()
mfi = 100 - 100 / (1 + mf_positive / mf_negative)

ad = (((data_30min["close"]-data_30min["low"]) - (data_30min["high"]-data_30min["close"])) \
      *data_30min["volume"]/(data_30min["high"]-data_30min["low"])).cumsum()
adsoc = ad.ewm(span=3, min_periods=3).mean() - ad.ewm(span=14, min_periods=14).mean()
obv = (np.sign(data_30min["close"] - data_30min["close"].shift(1).fillna(0)) * data_30min["volume"]).cumsum()