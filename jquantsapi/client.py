import json
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Any, List, Mapping, Optional, Union

import pandas as pd  # type: ignore
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

from jquantsapi.client_json import JSONClient
from jquantsapi import constants, enums

if sys.version_info >= (3, 11):
    import tomllib
else:
    import tomli as tomllib


DatetimeLike = Union[datetime, pd.Timestamp, str]
_Data = Union[str, Mapping[str, Any]]


class TokenAuthRefreshBadRequestException(Exception):
    pass


class Client(JSONClient):
    """
    J-Quants API からデータを取得する
    ref. https://jpx.gitbook.io/j-quants-api/
    """

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
        super().__init__(refresh_token=refresh_token, mail_address=mail_address, password=password)

    def get_listed_info(self, code: str = "", date_yyyymmdd: str = "") -> pd.DataFrame:
        """
        Get listed companies

        Args:
            code: Issue code (Optional)
            date: YYYYMMDD or YYYY-MM-DD (Optional)

        Returns:
            pd.DataFrame: listed companies (sorted by Code)
        """
        j = super().get_listed_info(code=code, date_yyyymmdd=date_yyyymmdd)
        d = json.loads(j)
        df = pd.DataFrame.from_dict(d["info"])
        cols = constants.LISTED_INFO_COLUMNS
        if len(df) == 0:
            return pd.DataFrame([], columns=cols)
        df["Date"] = pd.to_datetime(df["Date"], format="%Y%m%d")
        df.sort_values("Code", inplace=True)
        return df[cols]

    def _get_listed_sections_raw(self) -> str:
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

    def get_listed_sections(self) -> pd.DataFrame:
        """
        セクター一覧を取得

        Args:
            N/A

        Returns:
            pd.DataFrame: セクター一覧
        """
        j = self._get_listed_sections_raw()
        d = json.loads(j)
        df = pd.DataFrame.from_dict(d["sections"])
        cols = constants.LISTED_SECTIONS_COLUMNS
        if len(df) == 0:
            return pd.DataFrame([], columns=cols)
        df.sort_values(constants.LISTED_SECTIONS_COLUMNS[0], inplace=True)
        return df[cols]

    def get_17_sectors(self) -> pd.DataFrame:
        """
        Get 17-sector code and name
        ref. https://jpx.gitbook.io/j-quants-api-en/api-reference/listed-api/17-sector

        Args:
            N/A

        Returns:
            pd.DataFrame: 17-sector code and name
        """
        df = pd.DataFrame(constants.SECTOR_17_DATA, columns=constants.SECTOR_17_COLUMNS)
        df.sort_values(constants.SECTOR_17_COLUMNS[0], inplace=True)
        return df

    def get_33_sectors(self) -> pd.DataFrame:
        """
        Get 33-sector code and name
        ref. https://jpx.gitbook.io/j-quants-api-en/api-reference/listed-api/33-sector

        Args:
            N/A

        Returns:
            pd.DataFrame: 33-sector code and name
        """
        df = pd.DataFrame(constants.SECTOR_33_DATA, columns=constants.SECTOR_33_COLUMNS)
        df.sort_values(constants.SECTOR_33_COLUMNS[0], inplace=True)
        return df

    @staticmethod
    def get_market_segments() -> pd.DataFrame:
        """
        Get market segment code and name

        Args:
            N/A

        Returns:
            pd.DataFrame: market segment code and name

        """

        df = pd.DataFrame(
            constants.MARKET_SEGMENT_DATA, columns=constants.MARKET_SEGMENT_COLUMNS
        )
        df.sort_values(constants.MARKET_SEGMENT_COLUMNS[0], inplace=True)
        return df

    def get_list(self, code: str = "", date_yyyymmdd: str = "") -> pd.DataFrame:
        """
        Get listed companies (incl English name for sectors/segments)

        Args:
            code: Issue code (Optional)
            date: YYYYMMDD or YYYY-MM-DD (Optional)

        Returns:
            pd.DataFrame: listed companies
        """
        df_list = self.get_listed_info(code=code, date_yyyymmdd=date_yyyymmdd)
        df_17_sectors = self.get_17_sectors()[
            ["Sector17Code", "Sector17CodeNameEnglish"]
        ]
        df_33_sectors = self.get_33_sectors()[
            ["Sector33Code", "Sector33CodeNameEnglish"]
        ]
        df_segments = self.get_market_segments()[
            ["MarketCode", "MarketCodeNameEnglish"]
        ]
        df_list = pd.merge(df_list, df_17_sectors, how="left", on=["Sector17Code"])
        df_list = pd.merge(df_list, df_33_sectors, how="left", on=["Sector33Code"])
        df_list = pd.merge(df_list, df_segments, how="left", on=["MarketCode"])
        df_list.sort_values("Code", inplace=True)
        return df_list

    def get_prices_daily_quotes(
        self,
        code: str = "",
        from_yyyymmdd: str = "",
        to_yyyymmdd: str = "",
        date_yyyymmdd: str = "",
    ) -> pd.DataFrame:
        """
        株価情報を取得

        Args:
            code: 銘柄コード
            from_yyyymmdd: 取得開始日
            to_yyyymmdd: 取得終了日
            date_yyyymmdd: 取得日

        Returns:
            pd.DataFrame: 株価情報 (Code, Date列でソートされています)
        """
        j = super().get_prices_daily_quotes(
            code=code,
            from_yyyymmdd=from_yyyymmdd,
            to_yyyymmdd=to_yyyymmdd,
            date_yyyymmdd=date_yyyymmdd,
        )
        d = json.loads(j)
        df = pd.DataFrame.from_dict(d["daily_quotes"])
        cols = constants.PRICES_DAILY_QUOTES_COLUMNS
        if len(df) == 0:
            return pd.DataFrame([], columns=cols)
        df["Date"] = pd.to_datetime(df["Date"], format="%Y%m%d")
        df.sort_values(["Code", "Date"], inplace=True)
        return df[cols]

    def get_price_range(
        self,
        start_dt: DatetimeLike = "20170101",
        end_dt: DatetimeLike = datetime.now(),
    ) -> pd.DataFrame:
        """
        全銘柄の株価情報を日付範囲指定して取得

        Args:
            start_dt: 取得開始日
            end_dt: 取得終了日

        Returns:
            pd.DataFrame: 株価情報 (Code, Date列でソートされています)
        """
        # pre-load id_token
        self.get_id_token()
        buff = []
        dates = pd.date_range(start_dt, end_dt, freq="D")
        with ThreadPoolExecutor(max_workers=self.MAX_WORKERS) as executor:
            futures = [
                executor.submit(
                    self.get_prices_daily_quotes, date_yyyymmdd=s.strftime("%Y%m%d")
                )
                for s in dates
            ]
            for future in as_completed(futures):
                df = future.result()
                buff.append(df)
        return pd.concat(buff).sort_values(["Code", "Date"])

    def get_fins_statements(
        self, code: str = "", date_yyyymmdd: str = ""
    ) -> pd.DataFrame:
        """
        財務情報取得

        Args:
            code: 銘柄コード
            date_yyyymmdd: 日付(YYYYMMDD or YYYY-MM-DD)

        Returns:
            pd.DataFrame: 財務情報 (DisclosedUnixTime列、DisclosureNumber列でソートされています)
        """
        j = super().get_fins_statements(code=code, date_yyyymmdd=date_yyyymmdd)
        d = json.loads(j)
        df = pd.DataFrame.from_dict(d["statements"])
        cols = constants.FINS_STATEMENTS_COLUMNS
        if len(df) == 0:
            return pd.DataFrame([], columns=cols)
        df["DisclosedDate"] = pd.to_datetime(df["DisclosedDate"], format="%Y-%m-%d")
        df["CurrentPeriodEndDate"] = pd.to_datetime(
            df["CurrentPeriodEndDate"], format="%Y-%m-%d"
        )
        df["CurrentFiscalYearStartDate"] = pd.to_datetime(
            df["CurrentFiscalYearStartDate"], format="%Y-%m-%d"
        )
        df["CurrentFiscalYearEndDate"] = pd.to_datetime(
            df["CurrentFiscalYearEndDate"], format="%Y-%m-%d"
        )
        df.sort_values(["DisclosedUnixTime", "DisclosureNumber"], inplace=True)
        return df[cols]

    def get_indices_topix(
        self,
        from_yyyymmdd: str = "",
        to_yyyymmdd: str = "",
    ) -> pd.DataFrame:
        """
        TOPIX Daily OHLC

        Args:
            from_yyyymmdd: starting point of data period (e.g. 20210901 or 2021-09-01)
            to_yyyymmdd: end point of data period (e.g. 20210907 or 2021-09-07)
        Returns:
            pd.DataFrame: TOPIX Daily OHLC (Sorted by "Date" column)
        """
        j = super().get_indices_topix(
            from_yyyymmdd=from_yyyymmdd, to_yyyymmdd=to_yyyymmdd
        )
        d = json.loads(j)
        df = pd.DataFrame.from_dict(d["topix"])
        cols = constants.INDICES_TOPIX_COLUMNS
        if len(df) == 0:
            return pd.DataFrame([], columns=cols)
        df["Date"] = pd.to_datetime(df["Date"], format="%Y%m%d")
        df.sort_values(["Date"], inplace=True)
        return df[cols]

    def get_markets_trades_spec(
        self,
        section: Union[str, enums.MARKET_API_SECTIONS] = "",
        from_yyyymmdd: str = "",
        to_yyyymmdd: str = "",
    ) -> pd.DataFrame:
        """
        Weekly Trading by Type of Investors

        Args:
            section: section name (e.g. "TSEPrime" or MARKET_API_SECTIONS.TSEPrime)
            from_yyyymmdd: starting point of data period (e.g. 20210901 or 2021-09-01)
            to_yyyymmdd: end point of data period (e.g. 20210907 or 2021-09-07)
        Returns:
            pd.DataFrame: Weekly Trading by Type of Investors (Sorted by "PublishedDate" and "Section" columns)
        """
        j = super().get_markets_trades_spec(
            section=section, from_yyyymmdd=from_yyyymmdd, to_yyyymmdd=to_yyyymmdd
        )
        d = json.loads(j)
        df = pd.DataFrame.from_dict(d["trades_spec"])
        cols = constants.MARKETS_TRADES_SPEC
        if len(df) == 0:
            return pd.DataFrame([], columns=cols)
        df["PublishedDate"] = pd.to_datetime(df["PublishedDate"], format="%Y-%m-%d")
        df["StartDate"] = pd.to_datetime(df["StartDate"], format="%Y-%m-%d")
        df["EndDate"] = pd.to_datetime(df["EndDate"], format="%Y-%m-%d")
        df.sort_values(["PublishedDate", "Section"], inplace=True)
        return df[cols]

    def get_fins_announcement(self) -> pd.DataFrame:
        """
        get fin announcement

        Args:
            N/A

        Returns:
            pd.DataFrame: Schedule of financial announcement
        """
        j = super().get_fins_announcement()
        d = json.loads(j)
        df = pd.DataFrame.from_dict(d["announcement"])
        cols = constants.FINS_ANNOUNCEMENT_COLUMNS
        if len(df) == 0:
            return pd.DataFrame([], columns=cols)
        df["Date"] = pd.to_datetime(df["Date"], format="%Y-%m-%d")
        df.sort_values(["Date", "Code"], inplace=True)
        return df[cols]

    def get_statements_range(
        self,
        start_dt: DatetimeLike = "20170101",
        end_dt: DatetimeLike = datetime.now(),
        cache_dir: str = "",
    ) -> pd.DataFrame:
        """
        財務情報を日付範囲指定して取得

        Args:
            start_dt: 取得開始日
            end_dt: 取得終了日
            cache_dir: CSV形式のキャッシュファイルが存在するディレクトリ

        Returns:
            pd.DataFrame: 財務情報 (DisclosedUnixTime列、DisclosureNumber列でソートされています)
        """
        # pre-load id_token
        self.get_id_token()

        buff = []
        futures = {}
        dates = pd.date_range(start_dt, end_dt, freq="D")
        with ThreadPoolExecutor(max_workers=self.MAX_WORKERS) as executor:
            for s in dates:
                # fetch data via API or cache file
                yyyymmdd = s.strftime("%Y%m%d")
                yyyy = yyyymmdd[:4]
                cache_file = f"fins_statements_{yyyymmdd}.csv.gz"
                if (cache_dir != "") and os.path.isfile(
                    f"{cache_dir}/{yyyy}/{cache_file}"
                ):
                    df = pd.read_csv(f"{cache_dir}/{yyyy}/{cache_file}")
                    buff.append(df)
                else:
                    future = executor.submit(
                        self.get_fins_statements, date_yyyymmdd=yyyymmdd
                    )
                    futures[future] = yyyymmdd
            for future in as_completed(futures):
                df = future.result()
                buff.append(df)
                yyyymmdd = futures[future]
                yyyy = yyyymmdd[:4]
                cache_file = f"fins_statements_{yyyymmdd}.csv.gz"
                if cache_dir != "":
                    # create year directory
                    os.makedirs(f"{cache_dir}/{yyyy}", exist_ok=True)
                    # write cache file
                    df.to_csv(f"{cache_dir}/{yyyy}/{cache_file}", index=False)

        return pd.concat(buff).sort_values(["DisclosedUnixTime", "DisclosureNumber"])
