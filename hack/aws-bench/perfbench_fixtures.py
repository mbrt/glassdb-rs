"""Builders of schema-version 2 perfbench results for the tests."""

from __future__ import annotations

from typing import Any

SHAPE_NAMES = {
    "rwSingle": "rw-single",
    "rwMany": "rw-many",
    "roSingle": "ro-single",
    "roMulti": "ro-multi",
}
WORKLOADS = {"lo": "low-conflict", "hi": "high-conflict"}


def mixed_cell(
    mode: str = "lo",
    affinity: int = 100,
    database_limit: int = 4,
    workers: int = 8,
    shapes: list[dict[str, Any]] | None = None,
    **fields: Any,
) -> dict[str, Any]:
    """Return a mixed cell with the shapes under their configuration names."""
    cell = {
        "workload": WORKLOADS[mode],
        "affinityPct": affinity,
        "databaseLimit": database_limit,
        "databases": min(database_limit, workers),
        "workersPerShape": workers,
        "failures": 0,
        "shapes": [
            {**shape, "shape": SHAPE_NAMES.get(shape["shape"], shape["shape"])}
            for shape in shapes or []
        ],
    }
    cell.update(fields)
    return cell


def contention_cell(
    num_keys: int,
    overlap: int,
    *,
    committed: int,
    tx_per_sec: float,
    p50_ms: float = 10.0,
    p90_ms: float = 20.0,
    replays: int = 0,
    direct_candidates: int = 0,
    direct_landed: int = 0,
    worker_drain_ms: float = 0,
) -> dict[str, Any]:
    """Return a contention cell of `num_keys` keys, `overlap` of them shared."""
    per_tx = max(committed, 1)
    return {
        "failures": 0,
        "parameters": {"keys": num_keys, "shared": overlap},
        "measure": {"workerDrainMs": worker_drain_ms},
        "aggregateProtocol": {
            "replays": replays,
            "directCandidatesPerTx": direct_candidates / per_tx,
            "directLandedPerTx": direct_landed / per_tx,
        },
        "shapes": [
            {
                "shape": "contention-rmw",
                "committed": committed,
                "txPerSec": tx_per_sec,
                "p50Ms": p50_ms,
                "p90Ms": p90_ms,
            }
        ],
    }


def report(scenario: str, runs: list[dict[str, Any]], **fields: Any) -> dict[str, Any]:
    """Return a result envelope of `scenario`."""
    return {"schemaVersion": 2, "scenario": scenario, "runs": runs, **fields}
