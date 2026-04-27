"""Clerk authentication helpers for FastAPI routes."""

from __future__ import annotations

import base64
import json
import logging
import time
from threading import RLock
from typing import Any, Optional

import requests
from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding, rsa
from fastapi import Header

from src.api.settings import APISettings, get_settings

LOGGER = logging.getLogger(__name__)


class ClerkAuthError(Exception):
    """Raised when a Clerk token or user lookup cannot be trusted."""


def _b64url_decode(value: str) -> bytes:
    padding_len = (-len(value)) % 4
    return base64.urlsafe_b64decode(value + ("=" * padding_len))


def _decode_json_segment(value: str) -> dict[str, Any]:
    try:
        parsed = json.loads(_b64url_decode(value))
    except (ValueError, json.JSONDecodeError) as exc:
        raise ClerkAuthError("Invalid JWT encoding") from exc
    if not isinstance(parsed, dict):
        raise ClerkAuthError("Invalid JWT payload")
    return parsed


def _int_claim(payload: dict[str, Any], claim: str) -> int | None:
    value = payload.get(claim)
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise ClerkAuthError(f"Invalid {claim} claim") from exc


def _public_key_from_jwk(jwk: dict[str, Any]) -> rsa.RSAPublicKey:
    if jwk.get("kty") != "RSA":
        raise ClerkAuthError("Unsupported JWK type")
    try:
        n = int.from_bytes(_b64url_decode(str(jwk["n"])), "big")
        e = int.from_bytes(_b64url_decode(str(jwk["e"])), "big")
    except KeyError as exc:
        raise ClerkAuthError("Malformed JWK") from exc
    return rsa.RSAPublicNumbers(e=e, n=n).public_key()


def _public_key_from_pem(pem_value: str) -> rsa.RSAPublicKey:
    normalized = pem_value.replace("\\n", "\n").encode("utf-8")
    key = serialization.load_pem_public_key(normalized)
    if not isinstance(key, rsa.RSAPublicKey):
        raise ClerkAuthError("Clerk JWT key must be an RSA public key")
    return key


class ClerkVerifier:
    """Verifies Clerk session tokens and fetches server-controlled metadata."""

    def __init__(self) -> None:
        self._session = requests.Session()
        self._lock = RLock()
        self._jwks_cache: tuple[float, list[dict[str, Any]]] | None = None
        self._user_cache: dict[str, tuple[float, dict[str, Any]]] = {}
        self._pem_key_cache: tuple[str, rsa.RSAPublicKey] | None = None

    def clear_cache(self) -> None:
        with self._lock:
            self._jwks_cache = None
            self._user_cache.clear()
            self._pem_key_cache = None

    def authenticate(self, token: str, settings: APISettings) -> dict[str, Any]:
        payload = self.verify_session_token(token, settings)
        user_id = payload.get("sub")
        if not isinstance(user_id, str) or not user_id:
            raise ClerkAuthError("Missing subject")

        user = self.fetch_user(user_id, settings)
        user.setdefault("id", user_id)
        user["session_claims"] = payload
        return user

    def verify_session_token(self, token: str, settings: APISettings) -> dict[str, Any]:
        parts = token.split(".")
        if len(parts) != 3:
            raise ClerkAuthError("Malformed JWT")

        header = _decode_json_segment(parts[0])
        payload = _decode_json_segment(parts[1])
        signature = _b64url_decode(parts[2])
        signing_input = f"{parts[0]}.{parts[1]}".encode("ascii")

        if header.get("alg") != "RS256":
            raise ClerkAuthError("Unexpected JWT algorithm")

        public_key = self._resolve_public_key(header, settings)
        try:
            public_key.verify(signature, signing_input, padding.PKCS1v15(), hashes.SHA256())
        except InvalidSignature as exc:
            raise ClerkAuthError("Invalid JWT signature") from exc

        self._validate_claims(payload, settings)
        return payload

    def fetch_user(self, user_id: str, settings: APISettings) -> dict[str, Any]:
        if not settings.clerk_secret_key:
            return {"id": user_id, "public_metadata": {}}

        now = time.monotonic()
        ttl = settings.clerk_cache_ttl_seconds
        with self._lock:
            cached = self._user_cache.get(user_id)
            if cached and ttl > 0 and now - cached[0] <= ttl:
                return dict(cached[1])

        url = f"{settings.clerk_api_url.rstrip('/')}/v1/users/{user_id}"
        response = self._session.get(
            url,
            headers={"Authorization": f"Bearer {settings.clerk_secret_key}"},
            timeout=settings.clerk_request_timeout_seconds,
        )
        if response.status_code != 200:
            raise ClerkAuthError(f"Clerk user lookup failed: {response.status_code}")

        user = response.json()
        if not isinstance(user, dict):
            raise ClerkAuthError("Clerk user response was not an object")

        with self._lock:
            self._user_cache[user_id] = (now, dict(user))
        return user

    def _resolve_public_key(
        self, header: dict[str, Any], settings: APISettings
    ) -> rsa.RSAPublicKey:
        if settings.clerk_jwt_key:
            with self._lock:
                if self._pem_key_cache and self._pem_key_cache[0] == settings.clerk_jwt_key:
                    return self._pem_key_cache[1]
                key = _public_key_from_pem(settings.clerk_jwt_key)
                self._pem_key_cache = (settings.clerk_jwt_key, key)
                return key

        jwks = self._fetch_jwks(settings)
        kid = header.get("kid")
        if kid:
            matching = [key for key in jwks if key.get("kid") == kid]
        else:
            matching = jwks if len(jwks) == 1 else []
        if not matching:
            raise ClerkAuthError("No matching Clerk JWK")

        jwk = matching[0]
        if jwk.get("alg") not in (None, "RS256"):
            raise ClerkAuthError("Unexpected JWK algorithm")
        return _public_key_from_jwk(jwk)

    def _fetch_jwks(self, settings: APISettings) -> list[dict[str, Any]]:
        now = time.monotonic()
        ttl = settings.clerk_cache_ttl_seconds
        with self._lock:
            if self._jwks_cache and ttl > 0 and now - self._jwks_cache[0] <= ttl:
                return [dict(key) for key in self._jwks_cache[1]]

        url = self._jwks_url(settings)
        headers = {}
        if url.startswith(settings.clerk_api_url.rstrip("/")) and settings.clerk_secret_key:
            headers["Authorization"] = f"Bearer {settings.clerk_secret_key}"

        response = self._session.get(
            url,
            headers=headers,
            timeout=settings.clerk_request_timeout_seconds,
        )
        if response.status_code != 200:
            raise ClerkAuthError(f"Clerk JWKS lookup failed: {response.status_code}")

        body = response.json()
        keys = body.get("keys") if isinstance(body, dict) else None
        if not isinstance(keys, list) or not keys:
            raise ClerkAuthError("Clerk JWKS did not contain keys")

        jwks = [key for key in keys if isinstance(key, dict)]
        with self._lock:
            self._jwks_cache = (now, [dict(key) for key in jwks])
        return jwks

    def _jwks_url(self, settings: APISettings) -> str:
        if settings.clerk_jwks_url:
            return settings.clerk_jwks_url
        if settings.clerk_frontend_api_url:
            return f"{settings.clerk_frontend_api_url.rstrip('/')}/.well-known/jwks.json"
        if settings.clerk_secret_key:
            return f"{settings.clerk_api_url.rstrip('/')}/v1/jwks"
        raise ClerkAuthError("No Clerk JWT verification source configured")

    def _validate_claims(self, payload: dict[str, Any], settings: APISettings) -> None:
        now = int(time.time())
        skew = settings.clerk_jwt_clock_skew_seconds

        exp = _int_claim(payload, "exp")
        if exp is None or exp < now - skew:
            raise ClerkAuthError("Expired JWT")

        nbf = _int_claim(payload, "nbf")
        if nbf is not None and nbf > now + skew:
            raise ClerkAuthError("JWT not yet valid")

        if settings.clerk_issuer and payload.get("iss") != settings.clerk_issuer:
            raise ClerkAuthError("Unexpected issuer")

        if payload.get("sts") == "pending":
            raise ClerkAuthError("Pending Clerk session")

        authorized_parties = settings.clerk_authorized_parties or settings.cors_origins
        azp = payload.get("azp")
        if azp and authorized_parties and azp not in authorized_parties:
            raise ClerkAuthError("Unexpected authorized party")


_VERIFIER = ClerkVerifier()


def get_current_user(authorization: Optional[str] = Header(None)) -> dict[str, Any] | None:
    """Return the Clerk user for a verified session token, otherwise unauthenticated."""

    if not authorization or not authorization.startswith("Bearer "):
        return None

    token = authorization.removeprefix("Bearer ").strip()
    if not token:
        return None

    try:
        return _VERIFIER.authenticate(token, get_settings())
    except (ClerkAuthError, requests.RequestException, ValueError) as exc:
        LOGGER.info("Clerk auth failed closed: %s", exc)
        return None


def is_user_premium(user: Optional[dict[str, Any]]) -> bool:
    """Check Clerk server-controlled public metadata for premium entitlement."""

    if not user:
        return False

    metadata = user.get("public_metadata")
    if metadata is None:
        metadata = user.get("publicMetadata")
    if not isinstance(metadata, dict):
        return False

    return metadata.get("is_premium") is True
