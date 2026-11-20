"""OpenID Connect Authorization Code Flow с PKCE и состоянием в Redis."""
from __future__ import annotations

import base64
import hashlib
import json
import logging
import re
import secrets
import ssl
import time
from typing import Any
from urllib.parse import quote_plus, unquote, urlencode, urlsplit, urlunsplit

import httpx
from authlib.jose import JsonWebKey, JsonWebToken
from authlib.jose.errors import JoseError
from redis.exceptions import RedisError

from app import config
from app.db import get_redis_client
from app.user_profile import profile_claims

logger = logging.getLogger(__name__)

CALLBACK_PATH = "/api/auth/sso/callback"
FLOW_COOKIE_NAME = "oidc_flow"
_STATE_PREFIX = "sso:oidc:state:"
_FLOW_VALUE = re.compile(r"[A-Za-z0-9_-]{43}")
_TOKEN_METHODS = ("client_secret_basic", "client_secret_post")
_SIGNING_ALGS = ("RS256", "RS384", "RS512", "PS256", "PS384", "PS512", "ES256", "ES384", "ES512", "EdDSA")
_METADATA_TTL_SEC = 3600
_metadata_cache: tuple[str, float, dict[str, Any]] | None = None


class SSOError(Exception):
    """Ошибка конфигурации, провайдера или проверки входа."""


def _endpoint(value: Any, *, allow_query: bool = True) -> str:
    if not isinstance(value, str) or not value or any(ord(c) <= 32 or c == "\\" for c in value):
        raise SSOError("Некорректный URL в настройках OIDC")
    try:
        url = urlsplit(value)
        port = url.port
    except ValueError as exc:
        raise SSOError("Некорректный URL в настройках OIDC") from exc
    loopback = url.hostname in {"localhost", "127.0.0.1", "::1"}
    allowed_scheme = url.scheme == "https" or (url.scheme == "http" and config.SSO_ALLOW_HTTP and loopback)
    if not allowed_scheme or not url.hostname or url.username is not None or url.fragment:
        raise SSOError("OIDC требует HTTPS")
    if (port is not None and port < 1) or (not allow_query and url.query):
        raise SSOError("Некорректный URL в настройках OIDC")
    return value


def _request_json(method: str, url: str, **kwargs: Any) -> dict[str, Any]:
    try:
        verify = True
        if config.SSO_CA_FILE:
            verify = ssl.create_default_context()
            verify.load_verify_locations(cafile=config.SSO_CA_FILE)
        with httpx.Client(timeout=config.REQUEST_TIMEOUT_SEC, verify=verify) as client:
            response = client.request(method, url, **kwargs)
            response.raise_for_status()
            data = response.json()
        if not isinstance(data, dict):
            raise ValueError("Expected a JSON object")
        return data
    except (httpx.HTTPError, ValueError, OSError) as exc:
        logger.warning("OIDC provider request failed: %s", type(exc).__name__)
        raise SSOError("Провайдер OIDC не ответил ожидаемым образом") from exc


def _get_metadata() -> dict[str, Any]:
    global _metadata_cache
    issuer = _endpoint(config.SSO_ISSUER_URL, allow_query=False)
    now = time.monotonic()
    if _metadata_cache and _metadata_cache[0] == issuer and now - _metadata_cache[1] < _METADATA_TTL_SEC:
        return _metadata_cache[2]
    meta = _request_json("GET", f"{issuer.rstrip('/')}/.well-known/openid-configuration")
    if meta.get("issuer") != issuer:
        raise SSOError("Issuer в OIDC discovery не совпадает с настройками")
    for key in ("authorization_endpoint", "token_endpoint", "jwks_uri"):
        _endpoint(meta.get(key))
    _metadata_cache = (issuer, now, meta)
    return meta


def safe_return_path(target: str | None) -> str:
    if not target:
        return "/"
    for path in (target, unquote(target)):
        if not path.startswith("/") or path.startswith("//") or "\\" in path:
            return "/"
        if any(ord(char) < 32 or ord(char) == 127 for char in path):
            return "/"
    return target


def _state_key(state: str, browser_token: str) -> str:
    if not _FLOW_VALUE.fullmatch(state) or not _FLOW_VALUE.fullmatch(browser_token):
        raise SSOError("Запрос входа не найден или истёк")
    return f"{_STATE_PREFIX}{state}:{hashlib.sha256(browser_token.encode()).hexdigest()}"


def sso_build_authorize_url(return_to: str | None = None, force_login: bool = False) -> tuple[str, str]:
    redirect_uri = _endpoint(config.SSO_REDIRECT_URL, allow_query=False)
    if urlsplit(redirect_uri).path != CALLBACK_PATH:
        raise SSOError("Некорректный путь callback в настройках OIDC")
    meta = _get_metadata()
    state = secrets.token_urlsafe(32)
    browser_token = secrets.token_urlsafe(32)
    nonce = secrets.token_urlsafe(32)
    verifier = secrets.token_urlsafe(48)
    challenge = base64.urlsafe_b64encode(hashlib.sha256(verifier.encode()).digest()).rstrip(b"=").decode()
    payload = json.dumps({
        "issuer": config.SSO_ISSUER_URL, "nonce": nonce, "verifier": verifier,
        "return_to": safe_return_path(return_to),
    })
    client = get_redis_client()
    if client is None:
        raise SSOError("Хранилище состояния входа недоступно")
    try:
        client.setex(_state_key(state, browser_token), config.SSO_STATE_TTL_SEC, payload)
    except RedisError as exc:
        raise SSOError("Хранилище состояния входа недоступно") from exc
    params = {
        "client_id": config.SSO_CLIENT_ID, "redirect_uri": redirect_uri, "response_type": "code",
        "scope": "openid profile email", "state": state, "nonce": nonce,
        "code_challenge": challenge, "code_challenge_method": "S256",
    }
    if force_login:
        params["prompt"] = "login"
    endpoint = urlsplit(meta["authorization_endpoint"])
    query = "&".join(filter(None, (endpoint.query, urlencode(params))))
    return urlunsplit(endpoint._replace(query=query)), browser_token


def sso_consume_state(state: str, browser_token: str) -> dict[str, str]:
    key = _state_key(state, browser_token)
    client = get_redis_client()
    if client is None:
        raise SSOError("Хранилище состояния входа недоступно")
    try:
        raw = client.getdel(key)
    except RedisError as exc:
        raise SSOError("Хранилище состояния входа недоступно") from exc
    if raw is None:
        raise SSOError("Запрос входа не найден или истёк")
    data = json.loads(raw)
    if data["issuer"] != config.SSO_ISSUER_URL:
        raise SSOError("Настройки входа изменились")
    return data


def _token_auth_method(meta: dict[str, Any]) -> str:
    supported = meta.get("token_endpoint_auth_methods_supported", ["client_secret_basic"])
    configured = config.SSO_TOKEN_AUTH_METHOD
    if configured in _TOKEN_METHODS:
        return configured
    if configured == "auto" and isinstance(supported, list):
        for method in _TOKEN_METHODS:
            if method in supported:
                return method
    raise SSOError("Провайдер не поддерживает настроенный метод аутентификации клиента")


def sso_exchange_code(code: str, flow: dict[str, str]) -> dict[str, Any]:
    meta = _get_metadata()
    data = {
        "grant_type": "authorization_code", "code": code,
        "redirect_uri": config.SSO_REDIRECT_URL, "code_verifier": flow["verifier"],
    }
    auth = None
    if _token_auth_method(meta) == "client_secret_basic":
        auth = httpx.BasicAuth(quote_plus(config.SSO_CLIENT_ID), quote_plus(config.SSO_CLIENT_SECRET))
    else:
        data.update(client_id=config.SSO_CLIENT_ID, client_secret=config.SSO_CLIENT_SECRET)
    tokens = _request_json("POST", meta["token_endpoint"], data=data, auth=auth)
    id_token = tokens.get("id_token")
    if not isinstance(id_token, str) or not id_token:
        raise SSOError("Провайдер не вернул ID token")
    advertised = meta.get("id_token_signing_alg_values_supported", list(_SIGNING_ALGS))
    algs = [alg for alg in _SIGNING_ALGS if isinstance(advertised, list) and alg in advertised]
    if not algs:
        raise SSOError("Провайдер не объявил поддерживаемый алгоритм подписи")
    jwks = _request_json("GET", meta["jwks_uri"])
    try:
        claims = JsonWebToken(algs).decode(
            id_token,
            JsonWebKey.import_key_set(jwks),
            claims_options={
                "iss": {"essential": True, "value": config.SSO_ISSUER_URL},
                "aud": {"essential": True, "values": [config.SSO_CLIENT_ID]},
                "nonce": {"essential": True, "value": flow["nonce"]},
                "iat": {"essential": True}, "exp": {"essential": True}, "sub": {"essential": True},
            },
        )
        claims.validate(leeway=60)
    except (JoseError, TypeError, ValueError, KeyError) as exc:
        raise SSOError("Подпись или поля ID token не прошли проверку") from exc
    if claims["aud"] not in (config.SSO_CLIENT_ID, [config.SSO_CLIENT_ID]):
        raise SSOError("Некорректная аудитория ID token")
    if "azp" in claims and claims["azp"] != config.SSO_CLIENT_ID:
        raise SSOError("Некорректный клиент в ID token")
    sub = claims["sub"]
    if not isinstance(sub, str) or not sub.strip() or len(sub) > 255 or not sub.isascii():
        raise SSOError("Некорректный идентификатор пользователя")

    email = _text_claim(claims, "email")
    login_name = next((value for key in ("preferred_username", "username", "upn")
                       if (value := _text_claim(claims, key))), None)
    return {
        "issuer": config.SSO_ISSUER_URL, "subject": sub, "login_name": login_name, "email": email,
        "username": (login_name or email or "sso:" + hashlib.sha256(sub.encode()).hexdigest())[:64],
        "profile": profile_claims(claims),
    }


def _text_claim(claims: dict[str, Any], name: str) -> str | None:
    value = claims.get(name)
    if isinstance(value, str) and 0 < len(value.strip()) <= 320 and all(c.isprintable() for c in value):
        return value.strip()
    return None
