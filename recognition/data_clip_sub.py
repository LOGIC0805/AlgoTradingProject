import numpy as np
import pandas as pd
from .DataClip import Data_clip


class Analysis_for_history_data(Data_clip):
    def __init__(self, prices, threshold1, threshold2, threshold3, forward_days, t):
        """_summary_

        Parameters
        ----------
        prices : _type_
            _description_
        threshold1 : _type_
            _description_
        threshold2 : _type_
            _description_
        threshold3 : _type_
            _description_
        forward_days : _type_
            _description_ 未来n天的收益率
        t : _type_
            _description_ 选取的特征窗口的长度
        """
        super().__init__(prices, threshold1, threshold2, threshold3)
        self.forward_days = forward_days
        self.t = t
        self.windows_with_forward_returns = []
        self.windows_with_random_returns = []

    def history_analysis(self):
        """
        历史数据分析
        """
        self.data_analysis()
        self.get_inital_windows()
        self.deal_windows()
        self.trim_points_for_percent_b()
        self.add_return_info()
        self.trim_ret_points_for_prices()

    def get_forward_return(self):
        """
        计算未来收益率
        """
        forward_days = self.forward_days
        window_clips = self.window_clip3.copy()
        for j in window_clips:
            j["forward_returns_{}".format(forward_days)] = self.prices["close"]\
                .pct_change(forward_days).shift(-forward_days).loc[j["end"]]
        self.windows_with_forward_returns =  window_clips
    
    def get_random_window_forward_return(self):
        """
        计算随机窗口未来收益率 从该窗口结束到下一个窗口结束
        """
        windows = self.window_clip3.copy()
        windows_with_random_return = []
        for i in range(len(windows)-1):
            w1 = windows[i]
            w2 = windows[i+1]
            forward_ret = self.prices.loc[w2["end"]]["close"] / self.prices.loc[w1["end"]]["close"] - 1
            w1["forward_ret_random_window"] = forward_ret
            windows_with_random_return.append(w1)
        w2["forward_ret_random_window"] = np.nan
        windows_with_random_return.append(w2)
        self.windows_with_random_returns = windows_with_random_return
    
    @staticmethod
    def get_two_lists(clips):
        """
        合并两个列表
        """
        def zip_interleave(a, b):
            """
            交叉插入两个列表的值
            Parameters
            ----------
            a : _type_
                _description_
            b : _type_
                _description_

            Returns
            -------
            _type_
                _description_
            """
            result = []
            for i in range(max(len(a), len(b))):
                if i < len(a):
                    result.append(a[i][0])
                if i < len(b):
                    result.append(b[i][0])
            return result
        h_list = sorted((j["cum_ret"], i) for i, j in enumerate(clips) if j["is_finding_high"])
        l_list = sorted((j["cum_ret"], i) for i, j in enumerate(clips) if not j["is_finding_high"])
        positions = []
        for j in (h_list+l_list):
            positions.append(j[1])
        # 使用zip_interleave函数来实现交错排序列表h_list和l_list的整合
        result = zip_interleave(h_list, l_list)
        return positions, result

    def get_Slist(self, windows):
        """
        得到特征排列与S值
        Parameters
        ----------
        windows : _type_
            _description_

        Returns
        -------
        _type_
            _description_
        """
        t = self.t
        shape_list = []
        for i in range(0, len(windows)-t+1):
            clips = windows[i:i+t]
            p, v = self.get_two_lists(clips)
            shape_list.append(((p, v), clips[-1]))
        return shape_list
    

class Signal(Data_clip):

    def __init__(self, prices, threshold1, threshold2, threshold3, t):
        """_summary_

        Parameters
        ----------
        prices : _type_
            _description_
        threshold1 : _type_
            _description_
        threshold2 : _type_
            _description_
        threshold3 : _type_
            _description_
        t : _type_
            _description_ 特征选取的时间窗口
        """
        super().__init__(prices, threshold1, threshold2, threshold3)
        self.t = t

    def history_analysis(self):
        """
        历史数据处理
        """
        self.data_analysis()
        self.get_inital_windows()
        self.deal_windows()
        self.trim_points_for_percent_b()
        self.add_return_info()
        self.trim_ret_points_for_prices()
    
    def get_recent_windows(self):
        """
        最近的t个window
        """
        self.recent_windows = self.window_clip3[-self.t-1:-1]
    
    @staticmethod
    def get_two_lists(clips):
        def zip_interleave(a, b):
            result = []
            for i in range(max(len(a), len(b))):
                if i < len(a):
                    result.append(a[i][0])
                if i < len(b):
                    result.append(b[i][0])
            return result
        h_list = sorted((j["cum_ret"], i) for i, j in enumerate(clips) if j["is_finding_high"])
        l_list = sorted((j["cum_ret"], i) for i, j in enumerate(clips) if not j["is_finding_high"])
        positions = []
        for j in (h_list+l_list):
            positions.append(j[1])
        # 使用zip_interleave函数来实现交错排序列表h_list和l_list的整合
        result = zip_interleave(h_list, l_list)
        return positions, result

    def get_Slist(self):
        clips = self.recent_windows.copy()
        p, v = self.get_two_lists(clips)
        if clips:  # Check if the list is not empty
            shape_list = ((p, v), clips[-1])
            return shape_list
        else:
            # Handle the case where the list is empty
            return None  # or any other appropriate action

    @staticmethod
    def calculate_stats(numbers):
        """_summary_
        计算收益均值、胜率、盈亏比
        Parameters
        ----------
        numbers : _type_
            _description_

        Returns
        -------
        _type_
            _description_
        """
        total_sum = sum(numbers)
        mean = total_sum / len(numbers)
        positive_count = sum(1 for num in numbers if num > 0)
        positive_ratio = positive_count / len(numbers)
        positive_sum = sum(num for num in numbers if num > 0)
        negative_sum = sum(num for num in numbers if num < 0)
        profit_loss_ratio = positive_sum / abs(negative_sum) if negative_sum != 0 else float('inf')
        return mean, positive_ratio, profit_loss_ratio

    def get_similar_signals_in_history(self, history_data, gamma, num):
        """_summary_
        与历史数据做匹配 得到历史上相似形态出现后的收益情况
        Parameters
        ----------
        history_data : _type_
            _description_
        gamma : _type_
            _description_
        num : _type_
            _description_

        Returns
        -------
        _type_
            _description_
        """
        similar_signals = []
        forward_rets = []
        forward_random_rets = []
        if self.get_Slist() is not None:
            for h in history_data:
                if self.compare_two_shapes(h, self.get_Slist(), gamma):
                    similar_signals.append(h)
            
            if len(similar_signals) < num:
                return False
            else:
                for datas in similar_signals:
                    forward_rets.append(datas[1][list(datas[1].keys())[-2]])
                    forward_random_rets.append(datas[1][list(datas[1].keys())[-1]])
                return self.calculate_stats(forward_rets), self.calculate_stats(forward_random_rets)
        else:
            return False
    
    def generate_signals(self, history_data, gamma, num, type_="int"):
        """_summary_
        实盘生成信号
        Parameters
        ----------
        history_data : _type_
            _description_
        gamma : _type_
            _description_ d值差异的阈值
        num : _type_
            _description_ 对历史上信号的数目做限制
        type_ : str, optional
            _description_, by default "int" 固定持仓时间

        Returns
        -------
        _type_
            _description_
        """
        similar_signals = self.get_similar_signals_in_history(history_data, gamma, num)
        if similar_signals == False:
            return self.prices.iloc[-1]["date"], 0, 0
        else:
            if type_ == "int":
                if (similar_signals[0][0] >0) and (similar_signals[0][1]>0.3) and (similar_signals[0][2]) > 1.5:
                    return self.prices.iloc[-1]["date"], 1, 5
                else:
                    return self.prices.iloc[-1]["date"], 0, 0
            else:
                if (similar_signals[1][0] >0) and (similar_signals[1][1]>0.3) and (similar_signals[1][2]) > 1.5:
                    return self.prices.iloc[-1]["date"], 1, 100
                else:
                    return self.prices.iloc[-1]["date"], 0, 100

    def calculate_d_scores(self, v1, v2):
        """
        计算D值
        """
        return 1/(len(v1)-1) * abs((abs(pd.Series(v1).diff()) - abs(pd.Series(v2).diff()))).sum()

    def compare_two_shapes(self, s1, s2, gamma):
        """
        比较两个形状
        gamma: 阈值
        """
        if s1[0][0] != s2[0][0]:
            return False
        else:
            d = self.calculate_d_scores(s1[0][1], s2[0][1])
            if d <= gamma:
                return True
            else:
                return False