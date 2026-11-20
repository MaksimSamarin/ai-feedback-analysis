"""Путь возврата после SSO-входа не должен уводить на чужой сайт."""
import pytest

from app.sso import safe_return_path


@pytest.mark.parametrize("target, expected", [
    (None, "/"), ("", "/"), ("/", "/"), ("/#/admin/models", "/#/admin/models"), ("/reports?x=1", "/reports?x=1"),
])
def test_relative_paths_are_kept(target, expected) -> None:
    assert safe_return_path(target) == expected


@pytest.mark.parametrize("target", [
    "https://evil.example", "//evil.example", r"/\evil.example", r"/x\y", "/\t/evil.example",
    "/\n/evil.example", "/\r//evil.example", "/\x7f", "javascript:alert(1)", "evil.example",
])
def test_external_or_obfuscated_targets_fall_back_to_root(target) -> None:
    assert safe_return_path(target) == "/"
