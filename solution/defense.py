"""
Online fault detector for the Data Siege stream.

Cost funnel per event: payload metadata + ctx.baseline + ctx.state are free,
then exactly one metered tool call (tools return exact values, so repeating a
call never adds information). Each handler combines single-dimension bound
checks with a pillar-specific composite/statistical check.
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
# Composite batch check: sum of squared z-scores (~chi-square, <=5 dof on
# clean data); 20.5 is the 5-dof p=0.001 point.
BATCH_CHI2_THRESHOLD = 20.5
BATCH_WARMUP = 8


def register(ctx):
    ctx.on("data_batch", check_data_batch)
    ctx.on("contract_checkpoint", check_contract_checkpoint)
    ctx.on("lineage_run", check_lineage_run)
    ctx.on("feature_materialization", check_feature_materialization)
    ctx.on("embedding_batch", check_embedding_batch)


def _ok(result):
    return isinstance(result, dict) and "error" not in result


def _verdict(pillar, reasons):
    if reasons:
        return Verdict(alert=True, pillar=pillar, reason="; ".join(reasons))
    return Verdict(alert=False, pillar=pillar)


def _mu_sigma(lo, hi):
    """Baselines are mean +/- 3 sigma, so recover (mean, sigma) from the band."""
    return (lo + hi) / 2.0, (hi - lo) / 6.0


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

    # Composite distance catches batches drifting on several dims at once
    # while each one stays inside its own 3-sigma band. Every dim is centred
    # on its clean mean (learned online, seeded from the baseline band where
    # one exists) so a normal batch scores ~sqrt(n_dims), not something
    # inflated by uncentred null/staleness terms.
    stats = ctx.state.setdefault("batch_stats", {})
    vec = {"rows": rows, "mean": mean, "std": prof["std_amount"], "null": null, "stale": stale}
    if not reasons:
        d2, n = _composite_d2(stats, vec, b)
        if d2 is not None and d2 > BATCH_CHI2_THRESHOLD:
            reasons.append(f"composite distance^2 {d2:.1f} over {n} dims > {BATCH_CHI2_THRESHOLD}")
    if not reasons:
        for k, x in vec.items():
            _welford_add(stats.setdefault(k, [0, 0.0, 0.0]), x)
    return _verdict(pillar, reasons)


def _welford_add(acc, x):
    acc[0] += 1
    delta = x - acc[1]
    acc[1] += delta / acc[0]
    acc[2] += delta * (x - acc[1])


def _composite_d2(stats, vec, b):
    """Sum of squared z-scores across dims with a usable clean estimate.
    Rows/mean fall back to the baseline band until enough clean batches have
    been seen; std/null/stale have no published centre, so they join only
    once learned online."""
    prior = {"rows": _mu_sigma(b["row_count_min"], b["row_count_max"]),
             "mean": _mu_sigma(b["mean_amount_min"], b["mean_amount_max"])}
    d2, n = 0.0, 0
    for k, x in vec.items():
        acc = stats.get(k)
        if acc and acc[0] >= BATCH_WARMUP:
            mu, sd = acc[1], math.sqrt(acc[2] / (acc[0] - 1))
        elif k in prior:
            mu, sd = prior[k]
        else:
            continue
        if sd <= 0:
            continue
        d2 += ((x - mu) / sd) ** 2
        n += 1
    return (d2, n) if n >= 2 else (None, n)


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
    return _verdict(pillar, reasons)


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
    return _verdict(pillar, reasons)


def check_feature_materialization(payload, ctx):
    pillar = "ai_infra"
    fd = ctx.tools.feature_drift(payload["feature_view"], payload["batch_id"])
    if not _ok(fd):
        return Verdict(alert=False, pillar=pillar, reason="feature drift unavailable")
    shift = fd["mean_shift_sigma"]
    reasons = []
    if shift > FEATURE_SHIFT_SIGMA_THRESHOLD:
        reasons.append(f"train/serve skew {shift:.3f} sigma > {FEATURE_SHIFT_SIGMA_THRESHOLD}")
    return _verdict(pillar, reasons)


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
    return _verdict(pillar, reasons)
