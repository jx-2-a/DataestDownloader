# -*- coding: utf-8 -*-
"""
UniversalDownloader - 通用文件下载器（支持本地 / 远程 SSH）
特点：
  - 初始化时传入 ssh_client（可选）和 session（可选）
  - 支持断点续传 + 文件大小完整性检查
  - 保留 download_list(url_list) 方法
  - 自动区分本地下载和远程下载
"""

import os
import time
import shlex
from tqdm import tqdm
from pathlib import Path, PurePosixPath
import requests
from datetime import datetime


class UniversalDownloader:
    def __init__(self, save_dir: str = None, ssh_client=None, session: requests.Session = None):
        """
        初始化通用下载器

        参数:
            save_dir     : str   → 保存目录（本地路径 或 远程路径）
            ssh_client   : paramiko.SSHClient → 如果是远程下载，必须传入已连接的 ssh client
            session      : requests.Session   → 已认证的 session（如 CDSE Token session），可选
        """
        self.ssh = ssh_client
        self.is_remote = ssh_client is not None
        if self.is_remote:
            self.sftp = self.ssh.open_sftp()
        else:
            self.sftp = None

        self.session = session or requests.Session()

        if save_dir:
            if self.is_remote:
                raw = str(save_dir)
                raw = raw.replace("\\", "/")  # Windows → Linux
                raw = raw.lstrip("/\\")  # 删除多余前缀
                raw = "/" + raw  # 强制 Linux 根路径
                self.save_dir = str(
                    PurePosixPath(raw)
                )
                self._ensure_remote_dir()
                print(f"✅ 初始化远程下载器 → 保存路径: {self.save_dir}")
            else:
                self.save_dir = str(Path(save_dir))
                os.makedirs(self.save_dir, exist_ok=True)
                print(f"✅ 初始化本地下载器 → 保存路径: {self.save_dir}")
        else:
            self.save_dir = None

    def _ensure_remote_dir(self, path=None):

        if path is None:
            path = self.save_dir

        remote_dir = str(
            PurePosixPath(
                str(path).replace("\\", "/")
            )
        )

        # ⭐ 更新 save_dir
        if path == self.save_dir:
            self.save_dir = remote_dir

        print(f"[DEBUG] ensure remote_dir = {remote_dir}")

        cmd = f'mkdir -p {shlex.quote(remote_dir)}'

        stdin, stdout, stderr = self.ssh.exec_command(cmd)

        err = stderr.read().decode().strip()

        if err:
            print(f"[ERROR] mkdir failed:")
            print(err)
        else:
            print(f"[OK] 远程目录已就绪 → {remote_dir}")

    def _get_remote_size(self, remote_path: str) -> int:
        """获取远程文件大小（真实调试版）"""

        remote_path = str(
            PurePosixPath(
                str(remote_path).replace("\\", "/")
            )
        )

        cmd = f'stat -c%s "{remote_path}"'

        # print("\n[DEBUG] 查询远程文件大小")
        # print("[DEBUG] path =", remote_path)
        # print("[DEBUG] cmd  =", cmd)

        stdin, stdout, stderr = self.ssh.exec_command(cmd)

        out = stdout.read().decode().strip()
        err = stderr.read().decode().strip()

        # print("[DEBUG] stdout =", out)
        # print("[DEBUG] stderr =", err)

        if err:
            # print("[DEBUG] stat 失败（通常是文件不存在）")
            return 0

        try:
            size = int(out)
        except:
            size = 0

        # print("[DEBUG] remote_size =", size)

        return size

    def _get_server_file_size(self, url: str) -> int:
        """获取服务器文件总大小"""
        try:
            # 优先使用 HEAD
            resp = self.session.head(url, allow_redirects=True, timeout=30)
            if resp.status_code == 200 and 'Content-Length' in resp.headers:
                return int(resp.headers['Content-Length'])

            # fallback Range 请求
            resp = self.session.get(url, headers={'Range': 'bytes=0-0'},
                                  allow_redirects=True, timeout=30)
            if resp.status_code in (206, 200) and 'Content-Length' in resp.headers:
                return int(resp.headers['Content-Length'])
        except:
            pass
        return 0

    def download_file(self,name:str, url: str, max_retry: int = 5) -> bool:
        """下载单个文件（自动判断本地/远程）"""
        # 获取正确的文件名
        filename = name
        if self.is_remote:
            save_path = str(PurePosixPath(self.save_dir) / filename)
        else:
            save_path = Path(self.save_dir) / filename
        attempt = 0
        while attempt < max_retry:
            try:
                # 获取已下载大小
                if self.is_remote:
                    local_size = self._get_remote_size(save_path)
                else:
                    local_size = os.path.getsize(save_path) if os.path.exists(save_path) else 0

                total_size = self._get_server_file_size(url)
                print("[DEBUG] 远程:",total_size,"vs 本地:", local_size)

                # 完整性检查：如果已下载且大小一致，则跳过
                if total_size > 0 and local_size >= total_size:
                    print(f"[{datetime.now().strftime('%H:%M:%S')}] ✅ 已完成，跳过: {filename} "
                          f"({total_size / 1024 / 1024:.1f} MB)")
                    return True

                print(f"[{datetime.now().strftime('%H:%M:%S')}] 开始下载 → {filename} "
                      f"(尝试 {attempt + 1}/{max_retry})")

                headers = {"Range": f"bytes={local_size}-"} if local_size > 0 else {}
                chunk_size = 4 * 1024 * 1024  # 4MB

                pbar = tqdm(total=total_size or None, initial=local_size, unit='B', unit_scale=True,
                            desc=filename[:50], dynamic_ncols=True, ascii=True)

                with self.session.get(url, stream=True, headers=headers, timeout=60) as r:
                    r.raise_for_status()

                    if self.is_remote:

                        with self.sftp.file(save_path, "ab") as f:
                            f.set_pipelined(True)
                            for chunk in r.iter_content(chunk_size=chunk_size):
                                if chunk:
                                    f.write(chunk)
                                    f.flush()
                                    pbar.update(len(chunk))
                    else:
                        # 本地模式：直接写入文件
                        mode = "ab" if local_size > 0 else "wb"
                        with open(save_path, mode) as f:
                            for chunk in r.iter_content(chunk_size=chunk_size):
                                if chunk:
                                    f.write(chunk)
                                    pbar.update(len(chunk))

                pbar.close()
                print(f"[{datetime.now().strftime('%H:%M:%S')}] ✅ 下载完成: {filename}\n")
                return True

            except Exception as e:
                attempt += 1
                print(f"下载失败 (第 {attempt} 次): {e}")
                time.sleep(5)

        print(f"❌ 最终失败: {filename}（可下次续传）\n")
        return False

    def download_list(self, url_list: list, save_dir=None):
        """顺序下载整个 URL 列表（保留你原来的方法风格）"""
        if not url_list:
            print("URL 列表为空！")
            return

        if save_dir:
            self.save_dir = save_dir
        if not self.is_remote:
            os.makedirs(self.save_dir,exist_ok=True)
        else:
            self._ensure_remote_dir()
        print(f"\n开始顺序下载 {len(url_list)} 个文件 → 保存到: {self.save_dir}")
        print("=" * 100)

        for i, (name, url) in enumerate(url_list, 1):
            print(f"当前任务:[{i:3d}/{len(url_list)}]")
            self.download_file(name, url)

        print("=" * 100)
        print(f"全部任务处理完成！共 {len(url_list)} 个文件。\n")

# ====================== 使用示例 ======================
# # 示例1：本地下载（CDSE 数据）
# def example_local():
#     # 先创建带 Token 的 session
#     session = requests.Session()
#     # ... 这里填你的获取 CDSE Token 逻辑 ...
#     # session.headers.update({"Authorization": f"Bearer {token}"})
#
#     downloader = UniversalDownloader(
#         save_dir=r"D:\TROPOMI_NO2\2022-08",
#         session=session
#     )
#
#     url_list = ["https://download....", "https://download...."]   # 你的下载链接列表
#     downloader.download_list(url_list)
#
#
# # 示例2：远程服务器下载
# def example_remote():
#     # ssh_client = your_paramiko_ssh_client
#     # session = your_cdse_session
#
#     downloader = UniversalDownloader(
#         save_dir="/data5/obs/TROPOMI_China/Level_2/NO2/2022-08",
#         ssh_client=ssh_client,
#         session=session
#     )
#
#     downloader.download_list(your_url_list)