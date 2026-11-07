import pytest

from src.api import settings


@pytest.fixture
def stripe_environment(monkeypatch):
    monkeypatch.setattr(settings, "_load_dotenv_if_available", lambda: None)
    for name in ("STRIPE_ACCOUNT_ID", "STRIPE_PRICE_ID", "STRIPE_SECRET_KEY"):
        monkeypatch.delenv(name, raising=False)
    settings.get_settings.cache_clear()
    yield
    settings.get_settings.cache_clear()


@pytest.mark.parametrize(
    ("key", "livemode"),
    [
        ("sk_live_fixture", True),
        ("rk_live_fixture", True),
        ("sk_test_fixture", False),
        ("rk_test_fixture", False),
        (None, None),
        ("pk_live_fixture", None),
        ("sk_live_", None),
        ("sk_live_malformed-key", None),
    ],
)
def test_stripe_settings_derive_mode_without_retaining_the_key(
    stripe_environment, monkeypatch, key, livemode
):
    monkeypatch.setenv("STRIPE_ACCOUNT_ID", "acct_current")
    monkeypatch.setenv("STRIPE_PRICE_ID", "price_monthly")
    if key is not None:
        monkeypatch.setenv("STRIPE_SECRET_KEY", key)

    config = settings.get_settings()
    assert config.stripe_livemode is livemode
    assert config.stripe_account_id == "acct_current"
    assert config.stripe_price_id == "price_monthly"
    assert "stripe_secret_key" not in config.model_dump()
    if key is not None:
        assert key not in repr(config)


@pytest.mark.parametrize(("account", "price"), [(None, None), ("price_wrong", "acct_wrong"), ("acct_", "price_")])
def test_missing_or_malformed_stripe_identifiers_remain_unconfigured(
    stripe_environment, monkeypatch, account, price
):
    if account is not None:
        monkeypatch.setenv("STRIPE_ACCOUNT_ID", account)
    if price is not None:
        monkeypatch.setenv("STRIPE_PRICE_ID", price)
    monkeypatch.setenv("STRIPE_SECRET_KEY", "sk_live_fixture")

    config = settings.get_settings()
    assert config.stripe_account_id is None
    assert config.stripe_price_id is None
    assert config.stripe_livemode is True
