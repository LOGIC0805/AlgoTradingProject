import pandas as pd
from typing import Tuple, List, Dict

class BacktestStrategy:
    def __init__(self, signal_df: pd.Series, price_df: pd.Series, max_holding_days: int, take_profit_ratio: float, stop_loss_ratio: float, initial_capital: float, transaction_cost_ratio: float):
        self.signal_df = signal_df
        self.price_df = price_df
        self.max_holding_days = max_holding_days
        self.take_profit_ratio = take_profit_ratio
        self.stop_loss_ratio = stop_loss_ratio
        self.initial_capital = initial_capital
        self.current_capital = initial_capital
        self.cash = initial_capital
        self.positions: List[Dict] = []
        self.capital_history = [initial_capital]
        self.shares = 0  # 持有的股票数量
        self.transaction_cost_ratio = transaction_cost_ratio  # 交易费率

    def run_backtest(self) -> float:
        """ 开始回测
        Returns
        -------
        float
            _description_
        """
        for i in range(len(self.signal_df)):
            signal = self.signal_df.iloc[i]
            current_price = self.price_df.iloc[i]
            position_value = self.shares * current_price  # 持仓价值
            self.current_capital = self.cash + position_value  # 更新当前资本
            if signal == 1:
                self.open_position(i)
            elif pd.isna(signal) and self.positions:
                self.check_close_position(i)
            self.capital_history.append(self.current_capital)
        return self.current_capital

    def open_position(self, idx: int) -> None:
        """ 开仓函数, 计算止盈止损值，并更新
        Parameters
        ----------
        idx : int
            _description_
        """
        entry_price = self.price_df.iloc[idx]
        take_profit_price, stop_loss_price = self.calculate_take_profit_stop_loss(entry_price)
        if self.positions:
            self.positions[0].update({
                'take_profit_price': take_profit_price,
                'stop_loss_price': stop_loss_price
            })
        else:
            shares_to_buy = self.current_capital / (entry_price * (1 + self.transaction_cost_ratio))  # 可购买的股票数量
            self.shares += shares_to_buy
            self.cash -= shares_to_buy * entry_price * (1 + self.transaction_cost_ratio)  # 更新当前资本,扣除交易费用
            self.positions.append({
                'entry_idx': idx,
                'entry_price': entry_price,
                'take_profit_price': take_profit_price,
                'stop_loss_price': stop_loss_price,
                'shares': shares_to_buy
            })

    def check_close_position(self, idx: int) -> None:
        """ 判断是否达到平仓条件
        Parameters
        ----------
        idx : int
            _description_
        """
        position = self.positions[0]
        exit_price = self.price_df.iloc[idx]
        if exit_price >= position['take_profit_price'] \
                or exit_price <= position['stop_loss_price'] \
                or (idx - position["entry_idx"]) > self.max_holding_days:
            self.close_position(idx)

    def close_position(self, idx: int) -> None:
        """ 平仓函数
        并计算PnL
        Parameters
        ----------
        idx : int
        """
        position = self.positions.pop(0)
        exit_price = self.price_df.iloc[idx]
        shares = position['shares']
        pnl = (exit_price - position['entry_price']) * shares * (1 - self.transaction_cost_ratio)  # 扣除交易费用
        self.shares -= shares
        self.cash += shares * exit_price * (1 - self.transaction_cost_ratio)  # 扣除交易费用
        print(f"Closed position at index {idx}, PnL: {pnl:.2f}")

    def calculate_take_profit_stop_loss(self, entry_price: float) -> Tuple[float, float]:
        """ 计算止盈止损线
        Parameters
        ----------
        entry_price : float
            _description_
        Returns
        -------
        Tuple[float, float]
            _description_
        """
        take_profit_price = entry_price * (1 + self.take_profit_ratio)
        stop_loss_price = entry_price * (1 - self.stop_loss_ratio)
        return take_profit_price, stop_loss_price