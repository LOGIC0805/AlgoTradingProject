import pandas as pd
import numpy as np
import warnings
import matplotlib.pyplot as plt
from recognition.DataClip import Data_clip
from recognition.data_clip_sub import Analysis_for_history_data, Signal

warnings.filterwarnings("ignore")

#划分测试集与训练集
data_30min = pd.read_parquet("./data/data_30min.parquet").reset_index()
train_data = data_30min[data_30min["date"]<="2022-01-01 09:30:00"]
test_data = data_30min[data_30min["date"]>="2022-01-01 09:30:00"]
clip = Analysis_for_history_data(train_data, 0.03, 0.05, 0.02, 5, 3)
clip.history_analysis()
clip.get_forward_return()
clip.get_random_window_forward_return()
final_idxs = [0]
for i in clip.window_clip3:
    final_idxs.append(i["idx"])
clip.prices["close"].loc[final_idxs].plot(figsize=(20, 10))
clip.prices["close"].plot(figsize=(20, 10),linestyle="--")
signal_list = []
clip = Analysis_for_history_data(train_data, 0.03, 0.05, 0.02, 5, 3)
clip.history_analysis()
clip.get_forward_return()
clip.get_random_window_forward_return()
history_data = clip.get_Slist(clip.windows_with_forward_returns)

# 逐日生成交易信号
print("processing")
for j in range(100, len(test_data)):
    data = test_data.iloc[:j]
    signal_data = Signal(data, 0.03, 0.05, 0.02, 3)
    signal_data.history_analysis()
    signal_data.get_recent_windows()
    signal_list.append(signal_data.generate_signals(history_data, 0.5, 5))
temp_test = pd.DataFrame(signal_list).set_index(0)[1]
sigs = temp_test[(temp_test!=temp_test.shift(1)) &(temp_test==1)].reindex(temp_test.index).fillna(method="ffill", limit=4).fillna(0)
prices = test_data.set_index("date")["open"].loc[sigs.index]
print("end")

def backtest(prices:pd.Series, weights:pd.Series, initial_capital=100000, transaction=0.002):
    """_summary_ 
    简易 on_bar 回测框架
    Parameters
    ----------
    prices : pd.Series
        _description_ 价格数据
    weights : pd.Series
        _description_ 信号数据
    initial_capital : int, optional
        _description_, by default 100000
    transaction : float, optional 交易手续费(双边)
        _description_, by default 0.002

    Returns
    -------
    _type_
        _description_
    """
    position_series = pd.Series()
    portfolio = pd.Series()
    cash = initial_capital
    equity = 0
    total_equity = cash + equity
    for i in range(len(prices)):
        if i == 0:
            prev_position = 0
        else:
            prev_position = position_series.iloc[i-1]
            equity = prev_position * prices.iloc[i]

        # 计算预估的交易成本
        estimated_transaction_cost = np.abs(prev_position - (total_equity * weights.iloc[i] / prices.iloc[i]))\
            * prices.iloc[i] * transaction
        # 扣除交易成本后再计算新仓位
        position = (total_equity-estimated_transaction_cost) * weights.iloc[i] / prices.iloc[i]
        cash += (prev_position - position) * prices.iloc[i]
        equity += (position - prev_position) * prices.iloc[i]
        cash -= np.abs(prev_position - position) * prices.iloc[i] * transaction
        total_equity = equity + cash
        position_series.loc[i] = position
        portfolio.loc[i] = total_equity
    portfolio.index = weights.index
    return portfolio

def calculate_perf_indicator(nav_series):
    """_summary_
    回测
    Parameters
    ----------
    nav_series : _type_, optional
        _description_, by default pd.Series:策略净值
    benchmarck : _type_, optional
        _description_, by default pd.Series:benchmark净值

    Returns
    -------
    pd.Series
        _description_:囊括回测得到相关风险指标的Series
    """
    backtest_results = {}
    ret_series = nav_series.pct_change()
    backtest_results["begin_date"] = pd.to_datetime(nav_series.index[0])
    backtest_results["end_date"] = pd.to_datetime(nav_series.index[-1])
    backtest_results["years_delta"] = ((backtest_results["end_date"] - backtest_results["begin_date"]).days+1)/365
    backtest_results["return"] = nav_series.iloc[-1] / nav_series.iloc[0]-1
    backtest_results["ann_return"] = (backtest_results["return"]+1)**(1/backtest_results["years_delta"])-1
    backtest_results["ann_volatility"] = ret_series.std()*np.sqrt(1/backtest_results["years_delta"]*len(nav_series.index))
    backtest_results["Sharpe"] = (backtest_results["ann_return"]) / backtest_results["ann_volatility"]
    backtest_results["max_drawdown"] = 1-(nav_series/nav_series.cummax()).min()
    backtest_results["Calmar"] = backtest_results["ann_return"] / backtest_results["max_drawdown"]
    backtest_results["mdd_end_date"] = nav_series.index[(nav_series/nav_series.cummax()).argmin()]
    backtest_results["mdd_begin_date"] = nav_series.index[(nav_series.loc[:backtest_results["mdd_end_date"]]).argmax()]
    backtest_results["nav at mdd"] = nav_series.loc[backtest_results["mdd_end_date"]]
    try:
        backtest_results["recover_date"] = nav_series[nav_series>nav_series.loc[backtest_results["mdd_begin_date"]]].index[1]
    except:
        backtest_results["recover_date"] = "not recoverd"
    return pd.Series(backtest_results)

# 回测与指标计算
nav = backtest(prices, sigs)
nav.plot(figsize=(20,10), grid=True)
print(calculate_perf_indicator(nav))