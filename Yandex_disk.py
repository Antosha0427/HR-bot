import logging
import os
import requests
from urllib.parse import quote
import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class YandexDiskManager:
    def __init__(self):
        self.token = os.getenv("YANDEX_DISK_TOKEN")
        self.base_url = "https://cloud-api.yandex.net/v1/disk"
        self.headers = {"Authorization": f"OAuth {self.token}"}

    async def upload_file(self, local_path: str, cloud_path: str) -> bool:
        try:
            response = requests.get(
                f"{self.base_url}/resources/upload?path={quote(cloud_path)}&overwrite=true",
                headers=self.headers
            )
            upload_url = response.json().get("href")
            with open(local_path, "rb") as f:
                requests.put(upload_url, files={"file": f})
            return True
        except Exception as e:
            logger.error(f"Upload error: {e}")
            return False

    async def set_public_access(self, cloud_path: str) -> bool:
        """Открывает публичный доступ к файлу"""
        try:
            response = requests.put(
                f"{self.base_url}/resources/publish",
                headers=self.headers,
                params={"path": quote(cloud_path)}
            )
            return response.status_code == 200
        except Exception as e:
            logger.error(f"Public access error: {e}")
            return False

    async def set_private_access(self, cloud_path: str) -> bool:
        """Закрывает публичный доступ к файлу"""
        try:
            response = requests.put(
                f"{self.base_url}/resources/unpublish",
                headers=self.headers,
                params={"path": quote(cloud_path)}
            )
            return response.status_code == 200
        except Exception as e:
            logger.error(f"Private access error: {e}")
            return False

    async def force_save_via_chrome(self, doc_url: str) -> bool:
        """Принудительное сохранение через Chrome"""
        driver = None
        try:
            options = Options()
            options.add_argument("--start-maximized")
            driver = webdriver.Chrome(options=options)
            driver.get(doc_url)
            time.sleep(5)

            body = driver.find_element(By.TAG_NAME, 'body')
            body.send_keys(Keys.CONTROL + 's')
            time.sleep(3)
            return True
        except Exception as e:
            logger.error(f"Chrome save error: {e}")
            return False
        finally:
            if driver:
                driver.quit()

    async def get_file_version(self, cloud_path: str) -> str:
        """Получает дату последнего изменения файла"""
        try:
            response = requests.get(
                f"{self.base_url}/resources?path={quote(cloud_path)}",
                headers=self.headers
            )
            if response.status_code == 200:
                return response.json().get("modified", "")
            return ""
        except Exception as e:
            logger.error(f"File version error: {e}")
            return ""
    async def download_file(self, cloud_path: str, local_path: str) -> bool:

        try:
            download_url = await self.get_download_link(cloud_path)
            response = requests.get(download_url)
            with open(local_path, "wb") as f:
                f.write(response.content)
            return True
        except Exception as e:
            logger.error(f"Download error: {e}")
            return False
    async def get_download_link(self, cloud_path: str) -> str:
        response = requests.get(
            f"{self.base_url}/resources/download?path={quote(cloud_path)}",
            headers=self.headers
        )
        return response.json().get("href", "")