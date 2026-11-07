import time

import pytest

from src.api import auth
from src.api.settings import APISettings


@pytest.fixture
def scoped_premium_user(monkeypatch):
    scope = APISettings(
        stripe_account_id="acct_current",
        stripe_price_id="price_monthly",
        stripe_livemode=True,
        paid_enrollment_enabled=False,
    )
    monkeypatch.setattr(auth, "get_settings", lambda: scope)
    return {
        "public_metadata": {
            "is_premium": True,
            "subscription_current_period_end": int(time.time()) + 3600,
            "stripe_customer_id": "cus_current",
            "stripe_customer_account_id": scope.stripe_account_id,
            "stripe_customer_livemode": scope.stripe_livemode,
            "stripe_entitlement": {
                "customer_id": "cus_current",
                "account_id": scope.stripe_account_id,
                "livemode": scope.stripe_livemode,
                "price_id": scope.stripe_price_id,
            },
        },
    }
