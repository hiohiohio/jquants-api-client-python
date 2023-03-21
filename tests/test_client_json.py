import datetime
from contextlib import nullcontext as does_not_raise
from unittest.mock import patch

import pytest

import jquantsapi


@pytest.mark.freeze_time("2022-09-08T22:00:00Z")
@pytest.mark.parametrize(
    "mail_address, password, refresh_token,"
    "env, isfile, load,"
    "exp_mail_address, exp_password, exp_refresh_token, exp_refresh_token_expire,"
    "exp_raise",
    (
        (
            None,
            None,
            None,
            {},
            [True, False, True, False],
            [{"dummy": {"mail_address": "mail_address"}}, {}],
            "",
            "",
            "",
            0,
            pytest.raises(ValueError),
        ),
        (
            None,
            None,
            None,
            {"JQUANTS_API_CLIENT_CONFIG_FILE": ""},
            [True, True, True, True],
            [
                {
                    "jquants-api-client": {
                        "mail_address": "mail_address@",
                        "password": "password",
                    }
                },
                {},
                {},
                {
                    "jquants-api-client": {
                        "mail_address": "mail_address_overwrite@",
                        "refresh_token": "refresh_token",
                    }
                },
            ],
            "mail_address_overwrite@",
            "password",
            "refresh_token",
            6,
            does_not_raise(),
        ),
        (
            None,
            None,
            None,
            {
                "JQUANTS_API_MAIL_ADDRESS": "mail_address_env@",
                "JQUANTS_API_PASSWORD": "password_env",
                "JQUANTS_API_REFRESH_TOKEN": "refresh_token_env",
            },
            [True, False, False, False],
            [
                {
                    "jquants-api-client": {
                        "mail_address": "mail_address@",
                        "password": "password",
                        "refresh_token": "refresh_token",
                    }
                },
            ],
            "mail_address_env@",
            "password_env",
            "refresh_token_env",
            6,
            does_not_raise(),
        ),
        (
            "mail@",
            "password",
            None,
            {},
            [False, False, False, False],
            [],
            "mail@",
            "password",
            "",
            0,
            does_not_raise(),
        ),
        (
            "mail@",
            None,
            None,
            {},
            [False, False, False, False],
            [],
            "mail@",
            "",
            "",
            0,
            pytest.raises(ValueError),
        ),
        (
            "mail",
            "password",
            None,
            {},
            [False, False, False, False],
            [],
            "mail",
            "password",
            "",
            0,
            pytest.raises(ValueError),
        ),
        (
            None,
            None,
            "token",
            {},
            [False, False, False, False],
            [],
            "",
            "",
            "token",
            6,
            does_not_raise(),
        ),
        (
            "mail_address_param@",
            "password_param",
            "refresh_token_param",
            {},
            [True, False, False, False],
            [
                {
                    "jquants-api-client": {
                        "mail_address": "mail_address@",
                        "password": "password",
                        "refresh_token": "refresh_token",
                    }
                },
            ],
            "mail_address_param@",
            "password_param",
            "refresh_token_param",
            6,
            does_not_raise(),
        ),
    ),
)
def test_client(
    mail_address,
    password,
    refresh_token,
    env,
    isfile,
    load,
    exp_mail_address,
    exp_password,
    exp_refresh_token,
    exp_refresh_token_expire,
    exp_raise,
):
    utcnow = datetime.datetime.now(datetime.timezone.utc)
    with exp_raise, patch.object(
        jquantsapi.JSONClient, "_is_colab", return_value=True
    ), patch.object(
        jquantsapi.client_json.os.path, "isfile", side_effect=isfile
    ), patch(
        "builtins.open"
    ), patch.dict(
        jquantsapi.client_json.os.environ, env, clear=True
    ), patch.object(
        jquantsapi.client_json.tomllib, "load", side_effect=load
    ):
        cli = jquantsapi.Client(
            refresh_token=refresh_token, mail_address=mail_address, password=password
        )
        assert cli._mail_address == exp_mail_address
        assert cli._password == exp_password
        assert cli._refresh_token == exp_refresh_token
        assert cli._refresh_token_expire == utcnow + datetime.timedelta(
            days=exp_refresh_token_expire
        )


@pytest.mark.parametrize(
    "init_mail_address, init_password, param_mail_address, param_password, exp_raise",
    (
        # use private variable via constructor
        ("m@", "p", None, None, does_not_raise()),
        ("", "", None, None, pytest.raises(ValueError)),
        ("m", "p", None, None, pytest.raises(ValueError)),
        # use parameter
        (None, None, "m@", "p", does_not_raise()),
        (None, None, "", "", pytest.raises(ValueError)),
        (None, None, "m", "p", pytest.raises(ValueError)),
        # overwrite
        ("m@", "p", "", "", pytest.raises(ValueError)),
        ("m@", "p", "m", "p", pytest.raises(ValueError)),
    ),
)
def test_get_refresh_token(
    init_mail_address, init_password, param_mail_address, param_password, exp_raise
):
    config = {
        "mail_address": "",
        "password": "",
        "refresh_token": "dummy_token",
    }

    with exp_raise, patch.object(
        jquantsapi.JSONClient, "_load_config", return_value=config
    ), patch.object(jquantsapi.JSONClient, "_post") as mock_post:
        mock_post.return_value.json.return_value = {"refreshToken": "ret_token"}

        cli = jquantsapi.Client(
            refresh_token="dummy_token",
            mail_address=init_mail_address,
            password=init_password,
        )
        # overwrite expire time
        cli._refresh_token_expire = datetime.datetime.now(datetime.timezone.utc)
        ret = cli.get_refresh_token(param_mail_address, param_password)
        assert ret == "ret_token"
