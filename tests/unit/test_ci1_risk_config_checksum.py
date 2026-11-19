"""CI-1: the risk-limits checksum is line-ending stable and still catches real tampering."""
import hashlib
import json
import shutil

import pytest

from src.core.risk_engine import RiskEngine

CFG = "config/risk-limits.json"


def test_committed_config_is_lf_and_hash_matches_anchor():
    raw = open(CFG, "rb").read()
    assert b"\r" not in raw                                   # LF only (also enforced by .gitattributes)
    assert hashlib.sha256(raw).hexdigest() == RiskEngine._EXPECTED_CHECKSUM
    RiskEngine()                                              # verification succeeds


def test_gitattributes_forces_lf_for_the_config():
    assert "config/risk-limits.json text eol=lf" in open(".gitattributes").read()


def test_changed_risk_limit_value_raises_tampered(tmp_path):
    cfg = json.loads(open(CFG, encoding="utf-8").read())
    cfg["max_leverage"] = cfg["max_leverage"] + 1
    bad = tmp_path / "risk-limits.json"
    bad.write_text(json.dumps(cfg, indent=2) + "\n", encoding="utf-8", newline="\n")
    eng = RiskEngine()
    eng.config_path = str(bad)
    with pytest.raises(RuntimeError, match="RISK_CONFIG_TAMPERED"):
        eng._verify_checksum()


def test_crlf_variant_is_not_silently_accepted(tmp_path):
    """The check is NOT loosened to ignore line endings: CRLF bytes are a different file."""
    crlf = tmp_path / "risk-limits.json"
    crlf.write_bytes(open(CFG, "rb").read().replace(b"\n", b"\r\n"))
    eng = RiskEngine()
    eng.config_path = str(crlf)
    with pytest.raises(RuntimeError, match="RISK_CONFIG_TAMPERED"):
        eng._verify_checksum()
