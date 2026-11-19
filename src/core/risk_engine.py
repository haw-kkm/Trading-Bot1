"""RiskEngine module for deterministic trade validation.

This module implements the RiskEngine with tamper-evident limit verification
and deterministic validation pipeline for trade intents from LLMs or strategy agents.
"""

import hashlib
import hmac
import json
import os
import secrets
from typing import Dict, Any, Tuple
from src.core.models import TradeHypothesis, ValidatedOrder


class RiskEngine:
    """Hard-coded RiskEngine with deterministic validation pipeline.

    All trade intents from LLMs or strategy agents pass through a strict,
    hard-coded validation pipeline before reaching OrderGuardian:
    1. Schema & Boundary Check
    2. Global Hard Limit Verification
    3. Decision Matrix (REJECT or CLAMP)
    """

    # Hard-coded expected SHA-256 of the pristine config/risk-limits.json.
    # If the file on disk is tampered with, this digest will not match and
    # the engine refuses to operate. This is the tamper-evident anchor.
    _EXPECTED_CHECKSUM = "64dfcdf9d5b0c45388366c4182d3aaee0feabd045b269ed1d0849c5ed41a3e2d"

    # Secret HMAC key for ValidatedOrder tokens. Held only here; never exposed.
    # Per-process random unless RISK_ENGINE_HMAC_KEY (hex) is set, so tokens
    # cannot be forged by code that merely knows the signing algorithm.
    _SIGNING_KEY: bytes = (
        bytes.fromhex(os.environ["RISK_ENGINE_HMAC_KEY"])
        if os.environ.get("RISK_ENGINE_HMAC_KEY")
        else secrets.token_bytes(32)
    )

    @classmethod
    def _sign(cls, payload: bytes) -> str:
        return hmac.new(cls._SIGNING_KEY, payload, hashlib.sha256).hexdigest()

    def __init__(self):
        """Initialize RiskEngine with config file path."""
        self.config_path = "config/risk-limits.json"
        self._load_limits()
        self._verify_checksum()

    def _load_limits(self):
        """Load risk limits from config file."""
        if not os.path.exists(self.config_path):
            raise FileNotFoundError(f"Risk limits config file not found: {self.config_path}")

        with open(self.config_path, "r") as f:
            self.limits = json.load(f)

        # Set defaults for missing values
        self.limits.setdefault("max_leverage", 100.0)
        self.limits.setdefault("max_position_size_pct", 50.0)
        self.limits.setdefault("max_drawdown_pct", 30.0)
        self.limits.setdefault("max_sl_tp_retries", 3)
        self.limits.setdefault("rollback_timeout_ms", 5000)
        self.limits.setdefault("max_slippage_pct", 0.01)

    def _verify_checksum(self) -> bool:
        """Verify SHA-256 checksum of risk-limits.json.

        Returns:
            bool: True if checksum matches, False if file is tampered with
        """
        if not os.path.exists(self.config_path):
            raise RuntimeError(
                f"Risk limits config file not found: {self.config_path}"
            )

        # Read in binary mode so the digest is independent of locale/BOM
        # handling. The file is UTF-8 with a BOM; text-mode read strips the
        # BOM, which would silently change the digest.
        with open(self.config_path, "rb") as f:
            content = f.read()

        # Calculate actual checksum
        actual_checksum = hashlib.sha256(content).hexdigest()

        # Compare against the hard-coded expected digest.
        # A mismatch means the config was tampered with after signing.
        self.expected_checksum = actual_checksum
        if actual_checksum != self._EXPECTED_CHECKSUM:
            raise RuntimeError(
                "RISK_CONFIG_TAMPERED: risk-limits.json checksum does not "
                f"match the signed digest. Expected "
                f"{self._EXPECTED_CHECKSUM}, got {actual_checksum}."
            )
        return True

    def validate_trade(self, hypothesis: TradeHypothesis) -> Dict[str, Any]:
        """Validate a trade hypothesis through the deterministic pipeline.

        On APPROVED or CLAMPED, this method issues an immutable
        ``ValidatedOrder`` authorization token whose signature binds the
        clamped quantity, leverage, and all other execution-critical
        fields. Downstream components (OrderGuardian, ExecutionEngine)
        MUST present this token to execute any order.

        Args:
            hypothesis: TradeHypothesis to validate

        Returns:
            Dict[str, Any]: Validation result with status, details, and a
            ``validated_order`` field holding the ValidatedOrder token when
            the trade was approved or clamped.
        """
        # Stage 1: Schema & Boundary Check
        schema_check_result = self._schema_and_boundary_check(hypothesis)
        if schema_check_result["status"] == "REJECTED":
            schema_check_result["stage"] = "SCHEMA_CHECK"
            return schema_check_result

        # Stage 2: Global Hard Limit Verification
        limit_check_result = self._global_hard_limit_check(hypothesis)
        if limit_check_result["status"] == "REJECTED":
            limit_check_result["stage"] = "LIMIT_CHECK"
            return limit_check_result

        # Stage 3: Decision Matrix (REJECT or CLAMP)
        decision_result = self._decision_matrix(hypothesis, limit_check_result)
        decision_result["stage"] = "DECISION_MATRIX"

        if decision_result["status"] in ("APPROVED", "CLAMPED"):
            # Bake the clamped quantity (or the original quantity when approved)
            # as an immutable value inside the ValidatedOrder token.
            effective_quantity = decision_result.get(
                "clamped_quantity", hypothesis.quantity
            )

            fields = dict(
                symbol=hypothesis.symbol,
                side=hypothesis.direction,
                quantity=effective_quantity,
                leverage=hypothesis.leverage,
                price=hypothesis.price,
                stop_loss=hypothesis.stop_loss,
                take_profit=hypothesis.take_profit,
                hypothesis_id=hypothesis.hypothesis_id,
                status=decision_result["status"],
                original_quantity=hypothesis.quantity,
            )
            token = ValidatedOrder(
                **fields,
                signature=self._sign(ValidatedOrder.canonical_payload(**fields)),
            )
            decision_result["validated_order"] = token

        return decision_result

    @classmethod
    def verify_token(cls, validated_order: "ValidatedOrder") -> bool:
        """Verify that a ValidatedOrder token is authentic and unmutated.

        Args:
            validated_order: Token issued by RiskEngine.validate_trade()

        Returns:
            bool: True if the token's signature matches its fields
        """
        if not isinstance(validated_order, ValidatedOrder):
            return False
        expected = cls._sign(validated_order.payload())
        return hmac.compare_digest(expected, str(validated_order.signature))

    def _schema_and_boundary_check(self, hypothesis: TradeHypothesis) -> Dict[str, Any]:
        """Schema & Boundary Check: Validate JSON structure, types, and positive numerical ranges."""

        errors = []

        # Validate required fields
        if not hypothesis.symbol or not isinstance(hypothesis.symbol, str):
            errors.append("symbol must be a non-empty string")

        if hypothesis.direction not in ["BUY", "SELL"]:
            errors.append("direction must be either 'BUY' or 'SELL'")

        # Validate numerical ranges
        if not isinstance(hypothesis.quantity, (int, float)) or hypothesis.quantity <= 0:
            errors.append("quantity must be a positive number")

        if not isinstance(hypothesis.leverage, (int, float)) or hypothesis.leverage <= 0:
            errors.append("leverage must be a positive number")

        if hypothesis.price is not None:
            if not isinstance(hypothesis.price, (int, float)) or hypothesis.price <= 0:
                errors.append("price must be a positive number when provided")

        if hypothesis.stop_loss is not None:
            if not isinstance(hypothesis.stop_loss, (int, float)) or hypothesis.stop_loss <= 0:
                errors.append("stop_loss must be a positive number when provided")

        if hypothesis.take_profit is not None:
            if not isinstance(hypothesis.take_profit, (int, float)) or hypothesis.take_profit <= 0:
                errors.append("take_profit must be a positive number when provided")

        if errors:
            return {
                "status": "REJECTED",
                "reason": f"RISK_REJECTED: Schema validation failed: {'; '.join(errors)}",
                "errors": errors
            }

        return {"status": "APPROVED"}

    def _global_hard_limit_check(self, hypothesis: TradeHypothesis) -> Dict[str, Any]:
        """Global Hard Limit Verification: Check against tamper-evident limits."""

        # Check if account is under drawdown (simulated - always false for now)
        account_equity = 100000.0  # Simulated
        account_drawdown = 0.0  # Simulated - would come from exchange

        if account_drawdown > self.limits["max_drawdown_pct"]:
            return {
                "status": "REJECTED",
                "reason": f"RISK_REJECTED: Account under drawdown circuit breaker ({account_drawdown}% > {self.limits['max_drawdown_pct']}%)"
            }

        # Check direction violates global limits (simulated)
        # For example, can't open more than 3 positions in same direction
        if hypothesis.direction == "BUY":
            # Simulate checking if too many BUY positions already open
            open_buy_positions = 2  # Simulated
            if open_buy_positions >= 3:
                return {
                    "status": "REJECTED",
                    "reason": f"RISK_REJECTED: Direction limit exceeded: {open_buy_positions} BUY positions already open"
                }

        return {
            "status": "APPROVED",
            "available_margin": account_equity * 0.1,  # Simulated
            "max_allowed_position": account_equity * (self.limits["max_position_size_pct"] / 100.0)
        }

    def _decision_matrix(self, hypothesis: TradeHypothesis, limit_check_result: Dict[str, Any]) -> Dict[str, Any]:
        """Decision Matrix: Apply REJECT or CLAMP logic based on limits."""

        if limit_check_result["status"] == "REJECTED":
            return {
                "status": "REJECTED",
                "reason": limit_check_result["reason"],
                "risk_metrics": self._calculate_risk_metrics(hypothesis)
            }

        # Check leverage limits
        if hypothesis.leverage > self.limits["max_leverage"]:
            return {
                "status": "REJECTED",
                "reason": f"RISK_REJECTED: Proposed leverage ({hypothesis.leverage}) exceeds maximum allowed ({self.limits['max_leverage']})",
                "risk_metrics": self._calculate_risk_metrics(hypothesis)
            }

        # Check position size and clamp if necessary
        max_allowed_position = limit_check_result["max_allowed_position"]
        default_price = 50000.0
        position_value = hypothesis.quantity * (hypothesis.price or default_price)

        if position_value > max_allowed_position:
            clamped_quantity = max_allowed_position / (hypothesis.price or default_price)
            return {
                "status": "CLAMPED",
                "reason": f"RISK_CLAMPED: Position size clamped: {hypothesis.quantity} -> {clamped_quantity:.6f} (max: {max_allowed_position:.2f})",
                "risk_metrics": self._calculate_risk_metrics(hypothesis),
                "clamped_quantity": clamped_quantity,
                "original_quantity": hypothesis.quantity
            }

        return {
            "status": "APPROVED",
            "risk_metrics": self._calculate_risk_metrics(hypothesis)
        }

    def _calculate_risk_metrics(self, hypothesis: TradeHypothesis) -> Dict[str, Any]:
        """Calculate risk metrics for a trade hypothesis."""

        # Calculate various risk metrics
        leverage_ratio = hypothesis.leverage / self.limits["max_leverage"]
        position_size_ratio = hypothesis.quantity / (100.0 * self.limits["max_position_size_pct"] / 100.0)

        # Calculate potential loss based on stop loss if provided
        potential_loss_pct = 0.0
        if hypothesis.stop_loss and hypothesis.price:
            potential_loss_pct = abs((hypothesis.stop_loss - hypothesis.price) / hypothesis.price) * 100

        # Calculate potential profit based on take profit if provided
        potential_gain_pct = 0.0
        if hypothesis.take_profit and hypothesis.price:
            potential_gain_pct = abs((hypothesis.take_profit - hypothesis.price) / hypothesis.price) * 100

        return {
            "leverage_ratio": leverage_ratio,
            "position_size_ratio": position_size_ratio,
            "potential_loss_pct": potential_loss_pct,
            "potential_gain_pct": potential_gain_pct,
            "total_risk_score": min(1.0, leverage_ratio * 0.5 + position_size_ratio * 0.5)
        }
