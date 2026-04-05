# -*- coding: utf-8 -*-
"""
CDSEQueryClient - Copernicus Data Space Ecosystem 通用查询器
功能：
  - 根据参数查询产品，返回下载 URL 列表
  - 生成简洁的 TXT 结果文件（方便阅读和后续使用）
  - 提供 get_cdse_session() 函数，与 UniversalDownloader 完美闭环
"""

import requests
import os
from datetime import datetime
from base_dsc import Dsc


class CDSEQueryClient(Dsc):
    def __init__(self, config):
        super().__init__()
        self.config = config
        self.catalog_url = "https://catalogue.dataspace.copernicus.eu/odata/v1/Products"
        self.top = 1000

    # 包装同名方法以便使用
    def get_url(self, start, freq)->list:
        satellite = self.config["satellite"]
        contains = self.config["contains"]
        output_dir = self.config["output_dir"]

        start_date, end_date = self.get_start_end(start,freq)
        return self.query_download_urls(
            satellite=satellite,
            contains=contains,
            start_date=start_date,
            end_date=end_date,
            output_dir=output_dir
            )

    def get_session(self):
        return self.get_cdse_session()



    def build_query_url(self, satellite: str, contains: str,
                        start_date: str, end_date: str, skip: int = 0) -> str:
        """构建 OData 查询 URL"""
        base = f"{self.catalog_url}?$filter="
        filter_str = (
            f"Collection/Name eq '{satellite}' "
            f"and contains(Name, '{contains}') "
            f"and ContentDate/Start gt {start_date}T00:00:00.000Z "
            f"and ContentDate/Start lt {end_date}T00:00:00.000Z"
        )
        url = f"{base}{filter_str}&$top={self.top}&$skip={skip}&$orderby=ContentDate/Start asc"
        return url

    def query_download_urls(self,
                            satellite: str = 'SENTINEL-5P',
                            contains: str = 'OFFL_L2__NO2',
                            start_date: str = '2023-01-01',
                            end_date: str = '2025-01-01',
                            output_dir: str = './cdse_query_results') -> list:
        """
        查询指定条件的产品，返回下载 URL 列表

        返回: list[(name,str)]   # 所有文件的直接下载地址
        """
        os.makedirs(output_dir, exist_ok=True)
        result_txt = os.path.join(output_dir,
                                  f"{satellite}_{contains}_{start_date}_to_{end_date}.txt")

        print(f"开始查询 {satellite} | {contains} | {start_date} ~ {end_date} ...\n")

        all_urls = []
        all_records = []  # 用于生成 TXT
        skip = 0
        page = 1

        while True:
            url = self.build_query_url(satellite, contains, start_date, end_date, skip)
            print(f"正在请求第 {page} 页 (skip={skip}) ...")

            try:
                resp = requests.get(url, timeout=60)
                resp.raise_for_status()
                data = resp.json()
            except Exception as e:
                print(f"请求失败: {e}")
                break

            items = data.get('value', [])
            if not items:
                print("没有更多数据了。")
                break

            for item in items:
                name = item.get('Name', 'N/A')
                size_bytes = item.get('ContentLength', 0)
                size_mb = round(size_bytes / (1024 * 1024), 2)
                content_date = item.get('ContentDate', {}).get('Start', 'N/A')
                prod_id = item.get('Id', 'N/A')

                download_url = f"https://download.dataspace.copernicus.eu/odata/v1/Products({prod_id})/$value"

                all_urls.append((name,download_url))
                all_records.append({
                    'name': name,
                    'size_mb': size_mb,
                    'size_bytes': size_bytes,
                    'date': content_date,
                    'url': download_url
                })

            print(f"第 {page} 页获取到 {len(items)} 条，累计 {len(all_urls)} 条")

            if len(items) < self.top:
                break

            skip += self.top
            page += 1

        if not all_urls:
            print("未查询到任何数据！")
            return []

        # 生成简洁 TXT 文件
        with open(result_txt, 'w', encoding='utf-8') as f:
            f.write(f"CDSE 查询结果 - {satellite} {contains}\n")
            f.write(f"时间范围: {start_date} 至 {end_date}\n")
            f.write(f"总文件数: {len(all_urls)}\n")
            f.write("=" * 100 + "\n\n")

            for i, rec in enumerate(all_records, 1):
                f.write(f"{i:4d}. 文件名: {rec['name']}\n")
                f.write(f"     大小 : {rec['size_mb']:.2f} MB ({rec['size_bytes']:,} bytes)\n")
                f.write(f"     时间 : {rec['date']}\n")
                f.write(f"     下载地址:\n     {rec['url']}\n")
                f.write("-" * 90 + "\n")

        print("\n" + "=" * 90)
        print(f"✅ 查询完成！共找到 **{len(all_urls)}** 个文件")
        print(f"📄 结果文件: {result_txt}")
        print("=" * 90)

        # 打印前 3 个示例
        print("\n前 3 个下载地址示例：")
        for rec in all_records[:3]:
            print(f"• {rec['name']}")
            print(f"  大小: {rec['size_mb']:.2f} MB")
            print(f"  URL : {rec['url'][:100]}...\n")

        return all_urls


    def get_cdse_session(self, email: str = None, password: str = None) -> requests.Session:
        """
        获取带 Access Token 的 requests.Session（推荐每次下载前调用，Token 有效期较短）

        返回: 已设置 Authorization 的 Session，可直接传给 UniversalDownloader
        """
        if not email:
            email = self.config.get("email")
        if not password:
            password = self.config.get("password")
        data = {
            "client_id": "cdse-public",
            "username": email,
            "password": password,
            "grant_type": "password",
        }
        try:
            r = requests.post(
                "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token",
                data=data,
                timeout=30
            )
            r.raise_for_status()
            token = r.json()["access_token"]

            session = requests.Session()
            session.headers.update({"Authorization": f"Bearer {token}"})
            print(f"[{datetime.now().strftime('%H:%M:%S')}] ✅ CDSE Token 获取成功，Session 已创建")
            return session
        except Exception as e:
            print(f"Token 获取失败: {e}")
            raise


# ====================== 使用示例 ======================

if __name__ == "__main__":
    client = CDSEQueryClient()

    # 查询示例（可灵活修改参数）
    urls = client.query_download_urls(
        satellite='SENTINEL-5P',
        contains='OFFL_L2__NO2',
        start_date='2023-01-01',
        end_date='2023-02-01',  # 先小范围测试
        output_dir='./S5P_NO2_query_results'
    )

    # 获取 Session（与下载器闭环）
    # session = client.get_cdse_session("kosssullivanromai29044@gmail.com", "7qFdKu8vVaEC9Z-")

    # 下载示例（配合 UniversalDownloader）
    # downloader = UniversalDownloader(save_dir=r"D:\TROPOMI_NO2\2023-01", session=session)
    # downloader.download_list(urls)