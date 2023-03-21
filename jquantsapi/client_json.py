import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterator, List, Mapping, Optional, Union

import requests
from requests.adapters import HTTPAdapter
from requests.exceptions import HTTPError
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)
from urllib3.util import Retry

from jquantsapi import enums

if sys.version_info >= (3, 11):
    import tomllib
else:
    import tomli as tomllib


DatetimeLike = Union[datetime, str]
_Data = Union[str, Mapping[str, Any]]


class TokenAuthRefreshBadRequestException(Exception):
    pass


def date_range(
    start_date: DatetimeLike, end_date: DatetimeLike, step_days: int = 1
) -> Iterator[datetime]:
    """
    Generate a range of dates between start_date and end_date (inclusive) with a step of step_days.

    :param start_date: The starting date of the range, as a datetime object | yyyymmdd str | yyyy-mm-dd str.
    :param end_date: The ending date of the range, as a datetime object | yyyymmdd str | yyyy-mm-dd str.
    :param step_days: The number of days between each date in the range. Default is 1, which means consecutive dates.
    :return: An iterator that yields datetime objects in the specified range.
    """
    if type(start_date) is str:
        if "-" in start_date:
            start_date = datetime.strptime(start_date, "%Y-%m-%d")
        else:
            start_date = datetime.strptime(start_date, "%Y%m%d")
    elif type(start_date) is not datetime:
        raise TypeError("start_date must be a datetime object or a string.")

    if type(end_date) is str:
        if "-" in end_date:
            end_date = datetime.strptime(end_date, "%Y-%m-%d")
        else:
            end_date = datetime.strptime(end_date, "%Y%m%d")
    elif type(end_date) is not datetime:
        raise TypeError("end_date must be a datetime object or a string.")

    current_date = start_date

    # Continue looping until the current_date is greater than the end_date
    while current_date <= end_date:
        # Yield the current_date, so it can be used by the caller in a loop
        yield current_date

        # Increment the current_date by the specified number of days (step_days)
        current_date += timedelta(days=step_days)


class JSONClient:
    """
    fetch data as json string from J-Quants API
    ref. https://jpx.gitbook.io/j-quants-api/
    """

    JQUANTS_API_BASE = "https://api.jpx-jquants.com/v1"
    MAX_WORKERS = 5
    RAW_ENCODING = "utf-8"

    def __init__(
        self,
        refresh_token: Optional[str] = None,
        *,
        mail_address: Optional[str] = None,
        password: Optional[str] = None,
    ) -> None:
        """
        Args:
            refresh_token: J-Quants API refresh token
            refresh_token_expiredat: refresh token expired_at
            mail_address: J-Quants API login email address
            password: J-Quants API login password
        """
        config = self._load_config()

        self._mail_address = config["mail_address"]
        if mail_address is not None:
            self._mail_address = mail_address

        self._password = config["password"]
        if password is not None:
            self._password = password

        self._refresh_token = config["refresh_token"]
        if refresh_token is not None:
            self._refresh_token = refresh_token

        self._refresh_token_expire = datetime.now(timezone.utc)
        if self._refresh_token != "":
            self._refresh_token_expire += timedelta(days=6)

        self._id_token = ""
        self._id_token_expire = datetime.now(timezone.utc)
        self._session: Optional[requests.Session] = None

        if ((self._mail_address == "") or (self._password == "")) and (
            self._refresh_token == ""
        ):
            raise ValueError(
                "Either mail_address/password or refresh_token is required."
            )
        if (self._mail_address != "") and ("@" not in self._mail_address):
            raise ValueError("mail_address must contain '@' charactor.")

    def _is_colab(self) -> bool:
        """
        Return True if running in colab
        """
        return "google.colab" in sys.modules

    def _load_config(self) -> dict:
        """
        load config from files and environment variables

        Args:
            N/A
        Returns:
            dict: configurations
        """
        config: dict = {}

        # colab config
        if self._is_colab():
            colab_config_path = (
                "/content/drive/MyDrive/drive_ws/secret/jquants-api.toml"
            )
            config = {**config, **self._read_config(colab_config_path)}

        # user default config
        user_config_path = f"{Path.home()}/.jquants-api/jquants-api.toml"
        config = {**config, **self._read_config(user_config_path)}

        # current dir config
        current_config_path = "jquants-api.toml"
        config = {**config, **self._read_config(current_config_path)}

        # env specified config
        if "JQUANTS_API_CLIENT_CONFIG_FILE" in os.environ:
            env_config_path = os.environ["JQUANTS_API_CLIENT_CONFIG_FILE"]
            config = {**config, **self._read_config(env_config_path)}

        # env vars
        config["mail_address"] = os.environ.get(
            "JQUANTS_API_MAIL_ADDRESS", config.get("mail_address", "")
        )
        config["password"] = os.environ.get(
            "JQUANTS_API_PASSWORD", config.get("password", "")
        )
        config["refresh_token"] = os.environ.get(
            "JQUANTS_API_REFRESH_TOKEN", config.get("refresh_token", "")
        )

        return config

    def _read_config(self, config_path: str) -> dict:
        """
        read config from a toml file

        Params:
            config_path: a path to a toml file
        """
        if not os.path.isfile(config_path):
            return {}

        with open(config_path, mode="rb") as f:
            ret = tomllib.load(f)

        if "jquants-api-client" not in ret:
            return {}

        return ret["jquants-api-client"]

    def _base_headers(self) -> dict:
        """
        J-Quants API にアクセスする際にヘッダーにIDトークンを設定
        """
        id_token = self.get_id_token()
        headers = {"Authorization": f"Bearer {id_token}"}
        return headers

    def _request_session(
        self,
        status_forcelist: Optional[List[int]] = None,
        allowed_methods: Optional[List[str]] = None,
    ) -> requests.Session:
        """
        requests の session 取得

        リトライを設定

        Args:
            status_forcelist: リトライ対象のステータスコード
            allowed_methods: リトライ対象のメソッド
        Returns:
            requests.session
        """
        if status_forcelist is None:
            status_forcelist = [429, 500, 502, 503, 504]
        if allowed_methods is None:
            allowed_methods = ["HEAD", "GET", "OPTIONS", "POST"]

        if self._session is None:
            retry_strategy = Retry(
                total=3,
                status_forcelist=status_forcelist,
                allowed_methods=allowed_methods,
            )
            adapter = HTTPAdapter(
                # 安全のため並列スレッド数に更に10追加しておく
                pool_connections=self.MAX_WORKERS + 10,
                pool_maxsize=self.MAX_WORKERS + 10,
                max_retries=retry_strategy,
            )
            self._session = requests.Session()
            self._session.mount("https://", adapter)

        return self._session

    def _get(self, url: str, params: Optional[dict] = None) -> requests.Response:
        """
        requests の get 用ラッパー

        ヘッダーにアクセストークンを設定
        タイムアウトを設定

        Args:
            url: アクセスするURL
            params: パラメーター

        Returns:
            requests.Response: レスポンス
        """
        s = self._request_session()

        headers = self._base_headers()
        ret = s.get(url, params=params, headers=headers, timeout=30)
        ret.raise_for_status()
        return ret

    def _post(
        self,
        url: str,
        data: Optional[_Data] = None,
        json: Optional[Any] = None,
        headers: Optional[dict] = None,
    ) -> requests.Response:
        """
        requests の get 用ラッパー

        タイムアウトを設定

        Args:
            url: アクセスするURL
            payload: 送信するデータ
            headers: HTTPヘッダ

        Returns:
            requests.Response: レスポンス
        """
        s = self._request_session()

        ret = s.post(url, data=data, json=json, headers=headers, timeout=30)
        ret.raise_for_status()
        return ret

    def get_refresh_token(
        self, mail_address: Optional[str] = None, password: Optional[str] = None
    ) -> str:
        """
        get J-Quants API refresh token

        Params:
            mail_address: J-Quants API login email address
            password: J-Quants API login password
        Returns:
            refresh_token: J-Quants API refresh token
        """
        if self._refresh_token_expire > datetime.now(timezone.utc):
            return self._refresh_token

        if mail_address is None:
            mail_address = self._mail_address
        if password is None:
            password = self._password

        if mail_address == "" or password == "":
            raise ValueError("mail_address/password are required")
        if (mail_address is not None) and ("@" not in mail_address):
            raise ValueError("mail_address must contain '@' charactor.")

        url = f"{self.JQUANTS_API_BASE}/token/auth_user"
        data = {
            "mailaddress": mail_address,
            "password": password,
        }
        ret = self._post(url, json=data)
        refresh_token = ret.json()["refreshToken"]
        self._refresh_token = refresh_token
        self._refresh_token_expire = datetime.now(timezone.utc) + timedelta(days=6)
        return self._refresh_token

    @retry(
        retry=retry_if_exception_type(TokenAuthRefreshBadRequestException),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=5, max=300),
    )
    def get_id_token(self, refresh_token: Optional[str] = None) -> str:
        """
        get J-Quants API id_token

        Params:
            refresh_token: J-Quants API refresh token
        Retruns:
            id_token: J-Quants API id token
        """
        if self._id_token_expire > datetime.now(timezone.utc):
            return self._id_token

        if refresh_token is not None:
            _refresh_token = refresh_token
        else:
            _refresh_token = self.get_refresh_token()

        url = (
            f"{self.JQUANTS_API_BASE}/token/auth_refresh?refreshtoken={_refresh_token}"
        )
        try:
            ret = self._post(url)
        except HTTPError as e:
            # retry if:
            # - refresh_token is not provided as a parameter
            # - error is 400 bad request (refresh_token expire)
            # - mail_address and password are provided
            if (
                refresh_token is None
                and e.response.status_code == 400
                and self._mail_address != ""
                and self._password != ""
            ):
                # clear tokens for the next try
                self._refresh_token = ""
                self._refresh_token_expire = datetime.now(timezone.utc)
                self._id_token = ""
                self._id_token_expire = datetime.now(timezone.utc)
                # raise for retrying
                raise TokenAuthRefreshBadRequestException(e)
            raise e
        id_token = ret.json()["idToken"]
        self._id_token = id_token
        self._id_token_expire = datetime.now(timezone.utc) + timedelta(hours=23)
        return self._id_token

    def get_listed_info(self, code: str = "", date_yyyymmdd: str = "") -> str:
        """
        Get listed companies raw API returns

        Args:
            code: Issue code (Optional)
            date: YYYYMMDD or YYYY-MM-DD (Optional)

        Returns:
            str: listed companies raw json string
        """
        url = f"{self.JQUANTS_API_BASE}/listed/info"
        params = {}
        if code != "":
            params["code"] = code
        if date_yyyymmdd != "":
            params["date"] = date_yyyymmdd
        ret = self._get(url, params)
        ret.encoding = self.RAW_ENCODING
        return ret.text

    def get_listed_sections(self) -> str:
        """
        Get listed sections raw API returns

        Args:
            N/A

        Returns:
            str: list of sections
        """
        url = f"{self.JQUANTS_API_BASE}/listed/sections"
        params: dict = {}
        ret = self._get(url, params)
        ret.encoding = self.RAW_ENCODING
        return ret.text

    def get_prices_daily_quotes(
        self,
        code: str = "",
        from_yyyymmdd: str = "",
        to_yyyymmdd: str = "",
        date_yyyymmdd: str = "",
    ) -> str:
        """
        get daily quotes raw API returns

        Args:
            code: 銘柄コード
            from_yyyymmdd: 取得開始日
            to_yyyymmdd: 取得終了日
            date_yyyymmdd: 取得日

        Returns:
            str: daily quotes
        """
        url = f"{self.JQUANTS_API_BASE}/prices/daily_quotes"
        params = {
            "code": code,
        }
        if date_yyyymmdd != "":
            params["date"] = date_yyyymmdd
        else:
            if from_yyyymmdd != "":
                params["from"] = from_yyyymmdd
            if to_yyyymmdd != "":
                params["to"] = to_yyyymmdd
        ret = self._get(url, params)
        ret.encoding = self.RAW_ENCODING
        return ret.text

    def get_price_range(
        self,
        start_dt: DatetimeLike = "20170101",
        end_dt: DatetimeLike = datetime.now(),
    ) -> Iterator[str]:
        """
        全銘柄の株価情報を日付範囲指定して取得

        Args:
            start_dt: 取得開始日
            end_dt: 取得終了日

        Returns:
            Iterator[str]: 株価情報 (not sorted)
        """
        # pre-load id_token
        self.get_id_token()
        dates = date_range(start_dt, end_dt)
        with ThreadPoolExecutor(max_workers=self.MAX_WORKERS) as executor:
            futures = [
                executor.submit(
                    self.get_prices_daily_quotes, date_yyyymmdd=s.strftime("%Y%m%d")
                )
                for s in dates
            ]
            for future in as_completed(futures):
                yield future.result()

    def get_fins_statements(self, code: str = "", date_yyyymmdd: str = "") -> str:
        """
        get fins statements raw API return

        Args:
            code: 銘柄コード
            date_yyyymmdd: 日付(YYYYMMDD or YYYY-MM-DD)

        Returns:
            str: fins statements
        """
        url = f"{self.JQUANTS_API_BASE}/fins/statements"
        params = {
            "code": code,
            "date": date_yyyymmdd,
        }
        ret = self._get(url, params)
        ret.encoding = self.RAW_ENCODING

        return ret.text

    def get_indices_topix(
        self,
        from_yyyymmdd: str = "",
        to_yyyymmdd: str = "",
    ) -> str:
        """
        TOPIX Daily OHLC raw API returns

        Args:
            from_yyyymmdd: starting point of data period (e.g. 20210901 or 2021-09-01)
            to_yyyymmdd: end point of data period (e.g. 20210907 or 2021-09-07)
        Returns:
            str: TOPIX Daily OHLC
        """
        url = f"{self.JQUANTS_API_BASE}/indices/topix"
        params = {}
        if from_yyyymmdd != "":
            params["from"] = from_yyyymmdd
        if to_yyyymmdd != "":
            params["to"] = to_yyyymmdd
        ret = self._get(url, params)
        ret.encoding = self.RAW_ENCODING
        return ret.text

    def get_markets_trades_spec(
        self,
        section: Union[str, enums.MARKET_API_SECTIONS] = "",
        from_yyyymmdd: str = "",
        to_yyyymmdd: str = "",
    ) -> str:
        """
        Weekly Trading by Type of Investors raw API returns

        Args:
            section: section name (e.g. "TSEPrime" or MARKET_API_SECTIONS.TSEPrime)
            from_yyyymmdd: starting point of data period (e.g. 20210901 or 2021-09-01)
            to_yyyymmdd: end point of data period (e.g. 20210907 or 2021-09-07)
        Returns:
            str: Weekly Trading by Type of Investors
        """
        url = f"{self.JQUANTS_API_BASE}/markets/trades_spec"
        params = {}
        if section != "":
            params["section"] = section
        if from_yyyymmdd != "":
            params["from"] = from_yyyymmdd
        if to_yyyymmdd != "":
            params["to"] = to_yyyymmdd
        ret = self._get(url, params)
        ret.encoding = self.RAW_ENCODING
        return ret.text

    def get_fins_announcement(self) -> str:
        """
        get fin announcement raw API returns

        Args:
            N/A

        Returns:
            str: Schedule of financial announcement
        """
        url = f"{self.JQUANTS_API_BASE}/fins/announcement"
        ret = self._get(url)
        ret.encoding = self.RAW_ENCODING
        return ret.text

    def get_statements_range(
        self,
        start_dt: DatetimeLike = "20170101",
        end_dt: DatetimeLike = datetime.now(),
    ) -> Iterator[str]:
        """
        財務情報を日付範囲指定して取得

        Args:
            start_dt: 取得開始日
            end_dt: 取得終了日

        Returns:
            Iterator[str]: 財務情報 (not sorted)
        """
        # pre-load id_token
        self.get_id_token()

        dates = date_range(start_dt, end_dt)
        with ThreadPoolExecutor(max_workers=self.MAX_WORKERS) as executor:
            futures = [
                executor.submit(
                    self.get_fins_statements, date_yyyymmdd=s.strftime("%Y%m%d")
                )
                for s in dates
            ]
            for future in as_completed(futures):
                yield future.result()
