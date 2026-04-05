import os
from pathlib import Path
from datetime import datetime, timedelta
from ssh_con import KeyboardInteractiveSSH
from universaldownloader import UniversalDownloader

class Downloader:
    def __init__(self):
        self.get_config()
    def get_config(self):
        # 未来可以通过配置文件输入
        # 时间左开右闭
        config = {
            "type": "tropomi",
            "mode": "remote", # remote / local
            "save_dir": "/data5/obs/TROPOMI_China/Level_2/NO2",#"/data4/non-backup/zwchen/Data/obs/TROPOMI/Level_2/NO2",#"/data5/obs/TROPOMI_China/Level_2/NO2",
            "start": 20230101,
            "end": 20250101,
            "subfilepath": "{year}/{month}",
            "zeropad": True,

            "tropomi":{
                "email": "kosssullivanromai29044@gmail.com",
                "password": "7qFdKu8vVaEC9Z-",
                "satellite": "SENTINEL-5P",
                "contains": "OFFL_L2__NO2",
                "output_dir": "./cdse_query_results"
            }
        }
        self.config = config


    def run(self):
        ssh = self.get_ssh("210.45.127.67", "zwchen")
        ins = self.get_ins(self.config[self.config["type"]])
        session = ins.get_cdse_session()
        dwl = UniversalDownloader(save_dir=self.config["save_dir"], ssh_client=ssh, session = session)
        cfg = self.config
        resolution = self.detect_time_resolution(cfg["subfilepath"])
        for dt in self.iter_time_by_subpath(cfg["start"], cfg["end"], resolution):
            subpath = self.build_subpath(dt,cfg["subfilepath"],cfg["zeropad"])
            full_dir = Path(cfg["save_dir"]) / subpath
            # 获取
            url_list = ins.get_url(dt, resolution)
            dwl.download_list(url_list, full_dir)
        print("下载完成！")

    def get_ins(self,config):
        type = self.config.get("type")
        if type in ["tropomi"]:
            from tropomi import CDSEQueryClient
            return CDSEQueryClient(config)
        else:
            raise ValueError(f"未知type:{type}")
    def get_ssh(self, REMOTE_IP, REMOTE_USER):
        mode = self.config.get("mode")
        if mode == "remote":
            ssh_lib = KeyboardInteractiveSSH(REMOTE_IP, REMOTE_USER)
            self.ssh = ssh_lib.connect()
            return self.ssh
        elif mode == "local":
            return None
        else:
            raise ValueError(f"mode:{mode}值不对，-> remote/ local")

    def iter_time_by_subpath(self, start, end, resolution):
        """
        自动根据 subfilepath 选择时间粒度

        Example:
            "{year}" -> year
            "{year}/{month}" -> month
            "{year}/{month}/{day}" -> day
        """

        if resolution == "year":
            yield from self.iter_years(start, end)

        elif resolution == "month":
            yield from self.iter_months(start, end)

        elif resolution == "day":
            yield from self.iter_days(start, end)
    def detect_time_resolution(self, subfmt):
        """
        根据 subfilepath 判断最小时间颗粒度

        return:
            "year" | "month" | "day"
        """

        if "{day}" in subfmt:
            return "day"

        elif "{month}" in subfmt:
            return "month"

        elif "{year}" in subfmt:
            return "year"

        else:
            raise ValueError(
                f"subfilepath 不包含时间字段: {subfmt}"
            )
    def iter_years(self, start, end):

        start = datetime.strptime(str(start), "%Y%m%d")
        end = datetime.strptime(str(end), "%Y%m%d")

        cur = start.replace(month=1, day=1)

        while cur < end:
            yield cur

            cur = cur.replace(year=cur.year + 1)
    def iter_months(self, start, end):

        start = datetime.strptime(str(start), "%Y%m%d")
        end = datetime.strptime(str(end), "%Y%m%d")

        cur = start.replace(day=1)

        while cur < end:
            yield cur

            if cur.month == 12:
                cur = cur.replace(year=cur.year + 1, month=1)
            else:
                cur = cur.replace(month=cur.month + 1)
    def iter_days(self, start, end):

        start = datetime.strptime(str(start), "%Y%m%d")
        end = datetime.strptime(str(end), "%Y%m%d")

        cur = start

        while cur < end:
            yield cur

            cur += timedelta(days=1)
    def build_subpath(self, dt, subfmt, zeropad=True):
        if zeropad:
            year = f"{dt.year:04d}"
            month = f"{dt.month:02d}"
            day = f"{dt.day:02d}"
        else:
            year = str(dt.year)
            month = str(dt.month)
            day = str(dt.day)

        return subfmt.format(
            year=year,
            month=month,
            day=day
        )

if __name__ == "__main__":
    dl = Downloader()
    dl.run()