"""
System resource probe utilities.

Purpose:
- Report the *effective* CPU and memory limits inside the running container
  (important on Railway / Docker where host CPUs != container quota).
- Report relevant runtime knobs (Celery concurrency, Joblib thread env).
"""

from __future__ import annotations

import os
from dataclasses import dataclass, asdict
from typing import Any, Dict, Optional, Tuple


def _read_text(path: str) -> Optional[str]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return f.read().strip()
    except Exception:
        return None


def _parse_cpu_max(cpu_max: str) -> Optional[Tuple[Optional[float], str]]:
    """
    Parse cgroup v2 cpu.max
    Example: "200000 100000" => quota=200000us, period=100000us => 2.0 CPUs
             "max 100000" => unlimited
    Returns: (cpus, raw)
    """
    if not cpu_max:
        return None
    parts = cpu_max.split()
    if len(parts) < 2:
        return None
    quota, period = parts[0], parts[1]
    if quota == "max":
        return (None, cpu_max)
    try:
        q = float(quota)
        p = float(period)
        if p <= 0:
            return None
        return (q / p, cpu_max)
    except Exception:
        return None


def _get_cgroup_cpu_limit() -> Dict[str, Any]:
    # Prefer cgroup v2
    cpu_max = _read_text("/sys/fs/cgroup/cpu.max")
    if cpu_max:
        parsed = _parse_cpu_max(cpu_max)
        return {
            "version": "v2",
            "cpu_max": cpu_max,
            "cpu_limit_cpus": parsed[0] if parsed else None,
        }

    # Fallback to cgroup v1
    quota = _read_text("/sys/fs/cgroup/cpu/cpu.cfs_quota_us")
    period = _read_text("/sys/fs/cgroup/cpu/cpu.cfs_period_us")
    cpu_limit = None
    try:
        if quota is not None and period is not None:
            q = float(quota)
            p = float(period)
            if q > 0 and p > 0:
                cpu_limit = q / p
    except Exception:
        cpu_limit = None

    return {
        "version": "v1",
        "cpu_cfs_quota_us": quota,
        "cpu_cfs_period_us": period,
        "cpu_limit_cpus": cpu_limit,
    }


def _get_cgroup_memory_limit() -> Dict[str, Any]:
    # Prefer cgroup v2
    mem_max = _read_text("/sys/fs/cgroup/memory.max")
    if mem_max:
        mem_bytes = None
        if mem_max != "max":
            try:
                mem_bytes = int(mem_max)
            except Exception:
                mem_bytes = None
        return {"version": "v2", "memory_max": mem_max, "memory_limit_bytes": mem_bytes}

    # Fallback to cgroup v1
    mem_limit = _read_text("/sys/fs/cgroup/memory/memory.limit_in_bytes")
    mem_bytes = None
    try:
        if mem_limit is not None:
            mem_bytes = int(mem_limit)
    except Exception:
        mem_bytes = None
    return {"version": "v1", "memory_limit_in_bytes": mem_limit, "memory_limit_bytes": mem_bytes}


def _get_affinity_count() -> Optional[int]:
    try:
        if hasattr(os, "sched_getaffinity"):
            return len(os.sched_getaffinity(0))
    except Exception:
        return None
    return None


def _get_effective_cpu_count() -> int:
    """
    Best-effort "effective CPUs" inside container.
    Priority:
    - sched_getaffinity count
    - cgroup cpu limit (rounded down, min 1)
    - os.cpu_count
    """
    affinity = _get_affinity_count()
    if affinity and affinity > 0:
        return affinity

    cgroup = _get_cgroup_cpu_limit()
    limit = cgroup.get("cpu_limit_cpus")
    try:
        if isinstance(limit, (int, float)) and limit and limit > 0:
            return max(1, int(limit))
    except Exception:
        pass

    return os.cpu_count() or 1


@dataclass(frozen=True)
class ResourceProbe:
    python: Dict[str, Any]
    cgroup_cpu: Dict[str, Any]
    cgroup_memory: Dict[str, Any]
    env: Dict[str, Any]
    computed: Dict[str, Any]


def probe_resources() -> Dict[str, Any]:
    cpu_count = os.cpu_count() or 1
    affinity = _get_affinity_count()
    cgroup_cpu = _get_cgroup_cpu_limit()
    cgroup_mem = _get_cgroup_memory_limit()
    effective_cpus = _get_effective_cpu_count()

    # Known knobs we care about
    env = {
        "CELERY_CONCURRENCY": os.getenv("CELERY_CONCURRENCY"),
        "QC_VALIDATOR_MAX_WORKERS": os.getenv("QC_VALIDATOR_MAX_WORKERS"),
        "JOBLIB_MAX_NUM_THREADS": os.getenv("JOBLIB_MAX_NUM_THREADS"),
        "OMP_NUM_THREADS": os.getenv("OMP_NUM_THREADS"),
        "MKL_NUM_THREADS": os.getenv("MKL_NUM_THREADS"),
        "NUMEXPR_NUM_THREADS": os.getenv("NUMEXPR_NUM_THREADS"),
        "OPENAI_API_KEY_SET": bool(os.getenv("OPENAI_API_KEY")),
    }

    computed = {
        "os_cpu_count": cpu_count,
        "affinity_cpu_count": affinity,
        "effective_cpu_count": effective_cpus,
    }

    payload = ResourceProbe(
        python={"pid": os.getpid()},
        cgroup_cpu=cgroup_cpu,
        cgroup_memory=cgroup_mem,
        env=env,
        computed=computed,
    )
    return asdict(payload)

