class Data_clip:

    def __init__(self, prices, threshold1, threshold2, threshold3):
        """
        初始化
        Parameters
        ----------
        prices : _type_
            _description_ 价格数据
        threshold1 : _type_
            _description_ 第一次裁剪的阈值 %b阈值
        threshold2 : _type_
            _description_ 第二次裁剪的阈值 两笔之间极值的差异
        threshold3 : _type_
            _description_ 第三次裁剪的阈值 针对收益率的裁剪
        """
        self.prices = prices
        self.threshold1 = threshold1
        self.threshold2 = threshold2
        self.threshold3 = threshold3
        self.inital_windows = []
        self.window_clip1 = []
        self.window_clip2 = []
        self.windows_with_rets = []
        self.window_clip3 = []
    
    def data_analysis(self):
        """
        数据预处理
        """
        up_bound = self.prices["close"].rolling(20).mean() + 2*self.prices["close"].rolling(20).std()
        down_bound = self.prices["close"].rolling(20).mean() - 2*self.prices["close"].rolling(20).std()
        self.prices.loc[:, "percent_b"] = (self.prices.loc[:, "close"] - down_bound) / (up_bound - down_bound)
        self.prices = self.prices.dropna()
        self.prices.index = range(len(self.prices.index))
        self.percent_b =  self.prices["percent_b"]
    
    def get_inital_windows(self):
        """
        在给定的数据序列中查找所有满足条件的窗口,并记录每个窗口的最高点和最低点坐标。
        如果同时出现两个满足条件的最高点,以最后一个点为最高点;最低点同理。
        
        参数:
        data (list): 包含数据点的列表
        threshold (float): 允许的最大距离阈值
        
        返回:
        list: 包含所有窗口信息的列表,每个窗口用字典表示,包含起始点、结束点、最高点坐标和最低点坐标
        """
        data = self.percent_b
        windows = []
        start = 0
        end = start + 1
        max_height = data[start]
        max_idx = start
        min_height = data[start]
        min_idx = start
        is_finding_high = True  # 标记是否正在寻找最高点
        
        while end < len(data):
            if is_finding_high:
                # 寻找最高点
                if data[end] >= data[end - 1]:
                    # 价格上涨,向后移动窗口
                    end += 1
                    if data[end - 1] >= max_height:
                        max_height = data[end - 1]
                        max_idx = end - 1
                else:
                    if max_height- data[end] > self.threshold1:
                        windows.append({
                            'start': start,
                            'end': end,
                            'max_idx': max_idx,
                            'min_idx': min_idx,
                            "high": data[max_idx],
                            'low': data[min_idx],
                            "is_finding_high": is_finding_high
                        })
                        start = max_idx
                        max_height = data[start]
                        max_idx = start
                        min_height = data[start]
                        min_idx = start
                        is_finding_high = False
                        end = start +1
                    else:
                        end += 1
            else:
                # 寻找最低点
                if data[end] <= data[end - 1]:
                    # 价格下降,向后移动窗口
                    end += 1
                    if data[end - 1] <= min_height:
                        min_height = data[end - 1]
                        min_idx = end - 1
                else:
                    if data[end]- min_height > self.threshold1:
                        windows.append({
                            'start': start,
                            'end': end,
                            'max_idx': max_idx,
                            'min_idx': min_idx,
                            "high": data[max_idx],
                            'low': data[min_idx],
                            "is_finding_high": is_finding_high
                        })
                        start = min_idx
                        max_height = data[start]
                        max_idx = start
                        min_height = data[start]
                        min_idx = start
                        is_finding_high = True
                        end = start +1
                    else:
                        end += 1
        # 处理最后一个窗口
        if start < len(data):
            windows.append({
                'start': start,
                'end': len(data) - 1,
                'max_idx': max_idx,
                'min_idx': min_idx,
                "high": data[max_idx],
                'low': data[min_idx],
                'is_finding_high': is_finding_high
            })
        
        self.inital_windows = windows
    
    def deal_windows(self):
        """
        对窗口的数据做筛选
        """
        windows = self.inital_windows.copy()
        new_windows = []
        for i in windows:
            if i['max_idx'] == i['min_idx']:
                continue
            else: 
                if i["is_finding_high"]:
                    new_windows.append({
                        'height': i["high"],
                    'start': i["start"],
                        'end': i["end"],
                        'idx': i["max_idx"],
                        'is_finding_high': i["is_finding_high"]
                    })
                else:
                    new_windows.append({
                        'height': i["low"],
                    'start': i["start"],
                        'end': i["end"],
                        'idx': i["min_idx"],
                        'is_finding_high': i["is_finding_high"]
                    })
        self.window_clip1 = new_windows

    @ staticmethod
    def generate_window(w1,w2):
        """
        合成窗口
        Parameters
        ----------
        w1 : _type_
            _description_ 前一个
        w2 : _type_
            _description_ 后一个

        Returns
        -------
        _type_
            _description_ 合成好的
        """
        if w1["is_finding_high"] != w2["is_finding_high"]:
            new_window = {
                        'height': w1["height"],
                        'start': w1["start"],
                        'end': w2["end"],
                        'idx': w1["idx"],
                        'is_finding_high': w1["is_finding_high"]
                        }
        else:
            new_window = {
                        'height': w2["height"],
                        'start': w1["start"],
                        'end': w2["end"],
                        'idx': w2["idx"],
                        'is_finding_high': w2["is_finding_high"]
                        }
        return new_window
    
    def trim_points_for_percent_b(self):
        """
        针对%b指标 对窗口进行剪裁
        """
        windows = self.window_clip1.copy()
        delta = self.threshold2
        generated_windows = [windows[0]]
        for i in range(1, len(windows)):
            w1 = generated_windows[-1]
            w2 = windows[i]
            h1 = w1["height"]
            h2 = w2["height"]
            if w1["is_finding_high"] == w2["is_finding_high"]:
                generated_windows[-1] = self.generate_window(w1, w2)
            else:
                if abs(h1 - h2) < delta:
                    generated_windows[-1] = self.generate_window(w1, w2)
                else:
                    generated_windows.append(w2)
        self.window_clip2 = generated_windows

    def add_return_info(self):
        """
        给窗口添加收益率信息，以及走势标签
        """
        idxs = [0]
        for window in self.window_clip2:
            idxs.append(window["idx"])
        rets = self.prices["close"].pct_change().cumsum().loc[idxs].dropna().values
        prev_ret = 0
        windows = self.window_clip2.copy()
        for i in range(len(windows)):
            windows[i]["cum_ret"] = rets[i]
            if rets[i] >= prev_ret:
                windows[i]['is_finding_high'] = True
            else:
                windows[i]['is_finding_high'] = False
            prev_ret = rets[i]
        self.windows_with_rets = windows
    
    @ staticmethod
    def generate_window_rets(w1,w2):
        """
        合成窗口，添加收益率信息
        """
        if w1["is_finding_high"] != w2["is_finding_high"]:
            new_window = {
                        'height': w1["height"],
                        'start': w1["start"],
                        'end': w2["end"],
                        'idx': w1["idx"],
                        'is_finding_high': w1["is_finding_high"],
                        "cum_ret": w1["cum_ret"]
                        }
        else:
            if w1["is_finding_high"]:
                new_window = {
                    'height': w2["height"],
                    'start': w1["start"],
                    'end': w2["end"],
                    'idx': w2["idx"],
                    'is_finding_high': w2["is_finding_high"],
                    "cum_ret": max(w1["cum_ret"], w2["cum_ret"])
                }
            else:
                new_window = {
                    'height': w2["height"],
                    'start': w1["start"],
                    'end': w2["end"],
                    'idx': w2["idx"],
                    'is_finding_high': w2["is_finding_high"],
                    "cum_ret": min(w1["cum_ret"], w2["cum_ret"])
                }
        return new_window
 
    def trim_ret_points_for_prices(self):
        """
        剪裁收益率窗口
        """
        windows = self.windows_with_rets.copy()
        generated_windows = [windows[0]]
        for i in range(1, len(windows)):
            w1 = generated_windows[-1]
            w2 = windows[i]
            h1 = w1["cum_ret"]
            h2 = w2["cum_ret"]
            if w1["is_finding_high"] == w2["is_finding_high"]:
                generated_windows[-1] = self.generate_window_rets(w1, w2)
            else:
                if abs(h1 - h2) < self.threshold3:
                    generated_windows[-1] = self.generate_window_rets(w1, w2)
                else:
                    generated_windows.append(w2)
        self.window_clip3 = generated_windows