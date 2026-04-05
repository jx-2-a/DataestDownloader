from datetime import datetime
from dateutil.relativedelta import relativedelta
class Dsc:
    def __init__(self):
        pass
    # 必须支持的覆盖方法
    def get_url(self):
        pass

    def download(self):
        pass

    def get_session(self):
        pass

    # 方法工具
    def get_start_end(self, start: str, freq: str, mode: str = "ymd"):
        """
        根据 start、freq 和 mode 返回 start_ 和 end_（左闭右开区间）

        参数:
            start: 起始日期时间字符串，例如 "2023-01-01 00:00:00"
            freq:  "year" | "month" | "day"
            mode:  "ym" 或 "ymd" （默认 "ymd"）

        返回:
            tuple: (start_: str, end_: str)
        """
        # 验证 mode
        if mode not in ["ym", "ymd"]:
            raise ValueError("mode 参数只能是 'ym' 或 'ymd'")

        # 将字符串转为 datetime 对象
        dt = datetime.strptime(str(start), "%Y-%m-%d %H:%M:%S")

        if freq == "year":
            start_date = dt.replace(month=1, day=1)
            # 左闭右开：下一年的1月1日
            end_date = start_date + relativedelta(years=1)

        elif freq == "month":
            start_date = dt.replace(day=1)
            # 左闭右开：下个月的1日
            end_date = start_date + relativedelta(months=1)

        elif freq == "day":
            start_date = dt
            end_date = dt + relativedelta(days=1)  # 左闭右开：第二天00:00

        else:
            raise ValueError("freq 参数只能是 'year', 'month' 或 'day'")

        # 根据 mode 格式化输出
        if mode == "ymd":
            start_ = start_date.strftime("%Y-%m-%d")
            end_ = end_date.strftime("%Y-%m-%d")
        else:  # mode == "ym"
            start_ = start_date.strftime("%Y-%m")
            end_ = end_date.strftime("%Y-%m")

        return start_, end_