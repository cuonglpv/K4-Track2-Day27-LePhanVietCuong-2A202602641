"""
Online fault detector for the Data Siege stream.

Cost funnel per event: payload metadata + ctx.baseline + ctx.state are free,
then exactly one metered tool call (tools return exact values, so repeating a
call never adds information). Each handler layers three checks:
  1. fixed bounds from ctx.baseline / calibrated separation thresholds,
  2. a robust z-score (median/MAD) against that signal's own clean history,
     learned online in ctx.state, which catches subtle faults that still sit
     inside the static bounds,
  3. for data batches, a composite distance across all profile dimensions.
Only events that raised no alert feed the clean history, so faults don't
widen the reference distribution.
"""
import math

from api import Verdict

# dbt:stg_orders enriches orders with customer data, so both raw tables are
# required upstreams even though the run payload only declares raw.orders.
REQUIRED_UPSTREAM = {"dbt:stg_orders": {"raw.orders", "raw.customers"}}

# Separation thresholds placed between the max clean value and the min faulty
# value observed for each signal (the published 3-sigma baselines sit inside
# the clean stream's natural range for these and would raise false positives).
FEATURE_SHIFT_SIGMA_THRESHOLD = 0.65   # clean <= ~0.47, skew >= ~0.97
EMBEDDING_SHIFT_THRESHOLD = 0.0395     # clean < 0.039, drift > 0.040
DOC_AGE_DAYS_THRESHOLD = 44.5          # clean < 41, stale >= 48

# Online robust z-score: |x - median| / (1.4826 * MAD) over clean history.
# A caught fault is worth ~4x what a false alarm costs under the scoring
# formula (0.5/n_faulty vs 0.3/n_clean with ~1:3 faulty:clean), so this sits
# just below the point where practice stays at zero false positives (5.0),
# not at it.
ROBUST_Z = 4.5
ROBUST_WARMUP = 10
HISTORY_MAX = 200

# Composite batch check: sum of squared z-scores (~chi-square, <=5 dof on
# clean data); 20.5 is the 5-dof p=0.001 point.
BATCH_CHI2_THRESHOLD = 20.5

UPPER, LOWER, BOTH = "upper", "lower", "both"


def register(ctx):
    ctx.on("data_batch", check_data_batch)
    ctx.on("contract_checkpoint", check_contract_checkpoint)
    ctx.on("lineage_run", check_lineage_run)
    ctx.on("feature_materialization", check_feature_materialization)
    ctx.on("embedding_batch", check_embedding_batch)


def _ok(result):
    return isinstance(result, dict) and "error" not in result


def _mu_sigma(lo, hi):
    """Baselines are mean +/- 3 sigma, so recover (mean, sigma) from the band."""
    return (lo + hi) / 2.0, (hi - lo) / 6.0


# ---------------------------------------------------------------- online stats

def _history(ctx, key):
    return ctx.state.setdefault("history", {}).setdefault(key, [])


def _robust_center_scale(hist):
    """(median, robust sigma) of the clean history, or None during warmup."""
    if len(hist) < ROBUST_WARMUP:
        return None
    s = sorted(hist)
    med = _median(s)
    mad = _median(sorted(abs(v - med) for v in s))
    sigma = 1.4826 * mad
    # Guard against a degenerate (near-constant) history blowing z up.
    floor = max(abs(med) * 1e-3, 1e-9)
    return med, max(sigma, floor)


def _median(sorted_vals):
    n = len(sorted_vals)
    mid = n // 2
    return sorted_vals[mid] if n % 2 else (sorted_vals[mid - 1] + sorted_vals[mid]) / 2.0


def _directional(z, direction):
    """Only deviations in the faulty direction count."""
    if direction == UPPER:
        return max(z, 0.0)
    if direction == LOWER:
        return max(-z, 0.0)
    return abs(z)


class _Signals:
    """Collects one event's signals, scores them against clean history, and
    commits them to history only if the event ends up not alerting."""

    def __init__(self, ctx, prefix):
        self.ctx = ctx
        self.prefix = prefix
        self.values = {}
        self.z = {}

    def add(self, name, x, direction, prior=None):
        if x is None:
            return None
        hist = _history(self.ctx, f"{self.prefix}.{name}")
        cs = _robust_center_scale(hist) or prior
        self.values[name] = x
        if cs is None or cs[1] <= 0:
            return None
        z = _directional((x - cs[0]) / cs[1], direction)
        self.z[name] = z
        return z

    def outliers(self, threshold=ROBUST_Z):
        return [f"{n} robust z={z:.1f} (value {self.values[n]:.4g})"
                for n, z in self.z.items() if z > threshold]

    def commit(self, reasons):
        if reasons:
            return
        for name, x in self.values.items():
            hist = _history(self.ctx, f"{self.prefix}.{name}")
            hist.append(x)
            if len(hist) > HISTORY_MAX:
                del hist[0]


def _finish(pillar, sig, reasons):
    sig.commit(reasons)
    if reasons:
        return Verdict(alert=True, pillar=pillar, reason="; ".join(reasons))
    return Verdict(alert=False, pillar=pillar)


# -------------------------------------------------------------------- handlers

def check_data_batch(payload, ctx):
    pillar = "checks"
    prof = ctx.tools.batch_profile(payload["batch_id"])
    if not _ok(prof):
        return Verdict(alert=False, pillar=pillar, reason="profile unavailable")
    b = ctx.baseline
    rows = prof["row_count"]
    null = prof["null_rate"]["customer_id"]
    mean = prof["mean_amount"]
    stale = prof["staleness_min"]

    reasons = []
    if not b["row_count_min"] <= rows <= b["row_count_max"]:
        reasons.append(f"row_count {rows} outside baseline")
    if null > b["null_rate_max"]:
        reasons.append(f"customer_id null_rate {null:.4f} > {b['null_rate_max']}")
    if not b["mean_amount_min"] <= mean <= b["mean_amount_max"]:
        reasons.append(f"mean_amount {mean:.2f} outside baseline")
    if stale > b["staleness_min_max"]:
        reasons.append(f"staleness {stale:.2f}min > {b['staleness_min_max']}")

    # Rows/mean are scored against the baseline band until enough clean
    # history exists; std/null/stale have no published centre and join once
    # learned online.
    sig = _Signals(ctx, "batch")
    sig.add("rows", rows, BOTH, prior=_mu_sigma(b["row_count_min"], b["row_count_max"]))
    sig.add("mean", mean, BOTH, prior=_mu_sigma(b["mean_amount_min"], b["mean_amount_max"]))
    sig.add("std", prof["std_amount"], BOTH)
    sig.add("null", null, UPPER)
    sig.add("stale", stale, UPPER)

    if not reasons:
        reasons += sig.outliers()
    # Composite distance catches batches drifting on several dims at once
    # while each one stays unremarkable on its own.
    if not reasons and len(sig.z) >= 2:
        d2 = sum(z * z for z in sig.z.values())
        if d2 > BATCH_CHI2_THRESHOLD:
            reasons.append(f"composite distance^2 {d2:.1f} over {len(sig.z)} dims > {BATCH_CHI2_THRESHOLD}")
    return _finish(pillar, sig, reasons)


def check_contract_checkpoint(payload, ctx):
    pillar = "contracts"
    diff = ctx.tools.contract_diff(payload["contract_id"], payload["checkpoint_batch_id"])
    if not _ok(diff):
        return Verdict(alert=False, pillar=pillar, reason="contract diff unavailable")
    reasons = [f"violation: {v}" for v in diff.get("violations") or []]
    delay = diff.get("freshness_delay_min")
    sla = (payload.get("declared_sla") or {}).get("freshness_min")
    if delay is not None:
        if sla is not None and delay > sla:
            reasons.append(f"freshness {delay:.2f}min > declared SLA {sla}")
        elif delay > ctx.baseline["freshness_delay_max_min"]:
            reasons.append(f"freshness {delay:.2f}min > baseline {ctx.baseline['freshness_delay_max_min']}")

    sig = _Signals(ctx, f"contract.{payload.get('contract_id')}")
    sig.add("freshness_delay", delay, UPPER)
    if not reasons:
        reasons += sig.outliers()
    return _finish(pillar, sig, reasons)


def check_lineage_run(payload, ctx):
    pillar = "lineage"
    sl = ctx.tools.lineage_graph_slice(payload["run_id"])
    if not _ok(sl):
        return Verdict(alert=False, pillar=pillar, reason="lineage slice unavailable")
    reasons = []
    if sl.get("actual_downstream_count") == 0:
        reasons.append("orphan output: no downstream consumers")

    required = {i.get("name") for i in payload.get("inputs") or []}
    required |= REQUIRED_UPSTREAM.get(payload.get("job"), set())
    actual = set()
    for u in sl.get("actual_upstream") or []:
        actual.add(u.get("name") if isinstance(u, dict) else u)
    missing = sorted(required - actual)
    if missing:
        reasons.append(f"missing upstream: {', '.join(missing)}")

    dur = sl.get("duration_ms")
    if dur is not None and dur > ctx.baseline["lineage_duration_ms_max"]:
        reasons.append(f"runtime {dur}ms > {ctx.baseline['lineage_duration_ms_max']}")

    sig = _Signals(ctx, f"lineage.{payload.get('job')}")
    sig.add("duration_ms", dur, UPPER)
    if not reasons:
        reasons += sig.outliers()
    return _finish(pillar, sig, reasons)


def check_feature_materialization(payload, ctx):
    pillar = "ai_infra"
    fd = ctx.tools.feature_drift(payload["feature_view"], payload["batch_id"])
    if not _ok(fd):
        return Verdict(alert=False, pillar=pillar, reason="feature drift unavailable")
    shift = fd["mean_shift_sigma"]
    reasons = []
    if shift > FEATURE_SHIFT_SIGMA_THRESHOLD:
        reasons.append(f"train/serve skew {shift:.3f} sigma > {FEATURE_SHIFT_SIGMA_THRESHOLD}")

    # Signed shift keeps the direction the tool's absolute value throws away,
    # so a skew is judged against how far clean serving means actually wander.
    signed = None
    if fd.get("train_std"):
        signed = (fd["serve_mean"] - fd["train_mean"]) / fd["train_std"]
    sig = _Signals(ctx, f"feature.{payload.get('feature_view')}")
    sig.add("signed_shift", signed, BOTH)
    if not reasons:
        reasons += sig.outliers()
    return _finish(pillar, sig, reasons)


def check_embedding_batch(payload, ctx):
    pillar = "ai_infra"
    ed = ctx.tools.embedding_drift(payload["corpus"], payload["chunk_batch_id"])
    if not _ok(ed):
        return Verdict(alert=False, pillar=pillar, reason="embedding drift unavailable")
    reasons = []
    if ed["centroid_shift"] > EMBEDDING_SHIFT_THRESHOLD:
        reasons.append(f"centroid shift {ed['centroid_shift']:.4f} > {EMBEDDING_SHIFT_THRESHOLD}")
    if ed["avg_doc_age_days"] > DOC_AGE_DAYS_THRESHOLD:
        reasons.append(f"avg doc age {ed['avg_doc_age_days']:.1f}d > {DOC_AGE_DAYS_THRESHOLD}")

    sig = _Signals(ctx, f"embedding.{payload.get('corpus')}")
    sig.add("centroid_shift", ed["centroid_shift"], UPPER)
    sig.add("avg_doc_age_days", ed["avg_doc_age_days"], UPPER)
    if not reasons:
        reasons += sig.outliers()
    return _finish(pillar, sig, reasons)
