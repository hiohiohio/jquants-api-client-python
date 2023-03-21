from contextlib import nullcontext as does_not_raise
from datetime import datetime
from unittest.mock import MagicMock, call, patch

import pandas as pd
import pytest
from dateutil import tz

import jquantsapi


@pytest.mark.parametrize(
    "section, from_yyyymmdd, to_yyyymmdd, exp_params",
    (
        ("", "", "", {}),
        ("TSEPrime", "", "", {"section": "TSEPrime"}),
        (
            jquantsapi.MARKET_API_SECTIONS.TSE1st,
            "",
            "",
            {"section": "TSE1st"},
        ),
        ("", "20220101", "", {"from": "20220101"}),
        ("", "", "20220101", {"to": "20220101"}),
        (
            "TSEPrime",
            "20220101",
            "20220102",
            {"section": "TSEPrime", "from": "20220101", "to": "20220102"},
        ),
    ),
)
def test_get_markets_trades_spec(section, from_yyyymmdd, to_yyyymmdd, exp_params):
    config = {
        "mail_address": "",
        "password": "",
        "refresh_token": "dummy_token",
    }

    ret_value = '{"trades_spec": []}'
    exp_ret_len = 0
    exp_raise = does_not_raise()

    with exp_raise, patch.object(
        jquantsapi.JSONClient, "_load_config", return_value=config
    ), patch.object(jquantsapi.JSONClient, "_get") as mock_get:
        mock_get.return_value.text = ret_value

        cli = jquantsapi.Client()
        ret = cli.get_markets_trades_spec(
            section=section, from_yyyymmdd=from_yyyymmdd, to_yyyymmdd=to_yyyymmdd
        )
        args, _ = mock_get.call_args
        assert args[1] == exp_params
        assert len(ret) == exp_ret_len


def test_get_price_range():
    """
    get_price_range()を呼ぶ際、引数に様々な型が入っていても問題なく
    get_prices_daily_quotes()に単一の年月日が渡される事を確認する。
    """
    # テストではHTTP通信を行わず、
    # get_id_token()およびget_prices_daily_quotes()をモックすることで、
    # 実際に通信が発生しないようにする。
    mock = MagicMock(return_value=pd.DataFrame(columns=["Code", "Date"]))
    cli = jquantsapi.Client(refresh_token="dummy")
    cli.get_id_token = MagicMock()
    cli.get_prices_daily_quotes = mock  # get_prices_daily_quotes() をモックに置き換える
    formats = {
        # テストする期間はいつでも良いので、何かが起こりやすい閏日をテスト対象に含む
        "str_8digits": ("20200227", "20200302"),
        "str_split_by_hyphen": ("2020-02-27", "2020-03-02"),
        "datetime_without_tz": (datetime(2020, 2, 27), datetime(2020, 3, 2)),
        "datetime_with_tz": (
            datetime(2020, 2, 27, tzinfo=tz.gettz("Asia/Tokyo")),
            datetime(2020, 3, 2, tzinfo=tz.gettz("Asia/Tokyo")),
        ),
        "pd.Timestamp": (pd.Timestamp("2020-02-27"), pd.Timestamp("2020-03-02")),
    }

    for _fmt, (start, end) in formats.items():
        cli.get_price_range(start, end)

        # 呼び出しの履歴と、get_prices_daily_quotes()が呼ばれた際の年月日8桁の引数を比較
        assert mock.mock_calls == [
            call.get_prices_daily_quotes(date_yyyymmdd="20200227"),
            call.get_prices_daily_quotes(date_yyyymmdd="20200228"),
            call.get_prices_daily_quotes(date_yyyymmdd="20200229"),
            call.get_prices_daily_quotes(date_yyyymmdd="20200301"),
            call.get_prices_daily_quotes(date_yyyymmdd="20200302"),
        ]
        mock.reset_mock()
