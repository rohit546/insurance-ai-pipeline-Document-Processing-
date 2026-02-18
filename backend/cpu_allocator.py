"""
Dynamic CPU Allocator for QC and Summary Tasks
Manages CPU core allocation based on workload and priorities
"""
import os
from typing import Dict, Tuple
from pathlib import Path
from datetime import datetime


def read_cgroup_cpu_limit() -> int:
    """
    Read CPU limit from cgroup (Railway/container environment).
    Returns the effective CPU count based on cgroup limits.
    Falls back to os.cpu_count() if cgroup is not available.
    """
    try:
        # Try cgroup v2 first (newer systems)
        cpu_max_path = Path("/sys/fs/cgroup/cpu.max")
        if cpu_max_path.exists():
            content = cpu_max_path.read_text().strip()
            if content != "max":
                parts = content.split()
                if len(parts) >= 2:
                    quota = int(parts[0])
                    period = int(parts[1])
                    cpu_limit = quota / period
                    return int(cpu_limit)
        
        # Try cgroup v1 (older systems / Railway)
        quota_path = Path("/sys/fs/cgroup/cpu/cpu.cfs_quota_us")
        period_path = Path("/sys/fs/cgroup/cpu/cpu.cfs_period_us")
        
        if quota_path.exists() and period_path.exists():
            quota = int(quota_path.read_text().strip())
            period = int(period_path.read_text().strip())
            
            if quota > 0 and period > 0:
                cpu_limit = quota / period
                return int(cpu_limit)
    except Exception as e:
        print(f"[CPU Allocator] Warning: Could not read cgroup CPU limit: {e}")
    
    # Fallback to os.cpu_count()
    return os.cpu_count() or 1


def get_affinity_cpu_count() -> int:
    """
    Get CPU count based on affinity (what CPUs the process can actually use).
    More accurate than os.cpu_count() in containerized environments.
    """
    try:
        if hasattr(os, 'sched_getaffinity'):
            return len(os.sched_getaffinity(0))
    except Exception:
        pass
    return os.cpu_count() or 1


def get_effective_cpu_count() -> int:
    """
    Get the effective CPU count for this container.
    Uses the most conservative/accurate method available.
    
    Returns:
        int: Effective CPU count (what we should actually use)
    """
    # Try cgroup limit first (most accurate in containers)
    cgroup_limit = read_cgroup_cpu_limit()
    
    # Try affinity (what CPUs we can use)
    affinity_count = get_affinity_cpu_count()
    
    # Use os.cpu_count as fallback
    os_count = os.cpu_count() or 1
    
    # Use the minimum (most conservative)
    effective = min(cgroup_limit, affinity_count, os_count)
    
    print(f"[CPU Allocator] CPU Detection:")
    print(f"  - cgroup limit: {cgroup_limit}")
    print(f"  - affinity count: {affinity_count}")
    print(f"  - os.cpu_count(): {os_count}")
    print(f"  - effective (using): {effective}")
    
    return effective


def check_active_qc_tasks() -> int:
    """
    Check how many QC tasks are currently active or queued.
    
    Returns:
        int: Number of active QC tasks, or -1 if check failed (conservative)
    """
    try:
        from celery import current_app
        
        # Get inspect object
        inspect = current_app.control.inspect()
        
        # Get active tasks
        active = inspect.active()
        reserved = inspect.reserved()
        
        qc_count = 0
        
        # Count active QC tasks
        if active:
            for worker, tasks in active.items():
                for task in tasks:
                    task_name = task.get('name', '')
                    if 'qc' in task_name.lower():
                        qc_count += 1
        
        # Count queued/reserved QC tasks
        if reserved:
            for worker, tasks in reserved.items():
                for task in tasks:
                    task_name = task.get('name', '')
                    if 'qc' in task_name.lower():
                        qc_count += 1
        
        return qc_count
    except Exception as e:
        print(f"[CPU Allocator] Warning: Could not check active tasks: {e}")
        print(f"[CPU Allocator] Using conservative allocation to prevent oversubscription")
        return -1  # Signal: check failed, be conservative


def get_cpu_allocation_for_task(task_type: str) -> Tuple[int, str]:
    """
    Get CPU allocation for a task based on type and current workload.
    
    Args:
        task_type: 'qc' or 'summary'
    
    Returns:
        Tuple[int, str]: (cpu_cores, reason)
    """
    effective_cpu = get_effective_cpu_count()
    
    # QC allocation (simple: always 6 CPU if we have 8+, otherwise use 75% of available)
    if task_type == 'qc':
        if effective_cpu >= 8:
            qc_cpu = 6
            reason = "QC priority allocation (6/8 CPU)"
        else:
            qc_cpu = max(1, int(effective_cpu * 0.75))
            reason = f"QC priority allocation ({qc_cpu}/{effective_cpu} CPU, 75%)"
        
        print(f"\n{'='*80}")
        print(f"[CPU Allocator] QC Task Starting")
        print(f"{'='*80}")
        print(f"  Effective CPU: {effective_cpu} cores")
        print(f"  Allocated to QC: {qc_cpu} cores")
        print(f"  Reason: {reason}")
        print(f"{'='*80}\n")
        
        return qc_cpu, reason
    
    # Summary allocation (dynamic based on QC workload)
    elif task_type == 'summary':
        # Check if any QC tasks are active
        qc_active = check_active_qc_tasks()
        
        if qc_active == -1:
            # Check failed, be conservative: assume QC might be running
            if effective_cpu >= 8:
                summary_cpu = 2
                reason = f"Conservative allocation (QC detection failed, {summary_cpu}/8 CPU)"
            else:
                summary_cpu = max(1, int(effective_cpu * 0.25))
                reason = f"Conservative allocation (QC detection failed, {summary_cpu}/{effective_cpu} CPU, 25%)"
            qc_status = "unknown (detection failed)"
        elif qc_active > 0:
            # QC is running, give Summary the remaining cores
            if effective_cpu >= 8:
                summary_cpu = 2
                reason = f"Limited allocation ({qc_active} QC task(s) active, {summary_cpu}/8 CPU)"
            else:
                summary_cpu = max(1, int(effective_cpu * 0.25))
                reason = f"Limited allocation ({qc_active} QC task(s) active, {summary_cpu}/{effective_cpu} CPU, 25%)"
            qc_status = f"{qc_active} task(s) detected"
        else:
            # No QC running, Summary can use all available cores
            summary_cpu = effective_cpu
            reason = f"Opportunistic allocation (no QC active, using all {summary_cpu} CPU)"
            qc_status = "0 tasks (idle)"
        
        print(f"\n{'='*80}")
        print(f"[CPU Allocator] Summary Task Starting")
        print(f"{'='*80}")
        print(f"  Effective CPU: {effective_cpu} cores")
        print(f"  QC tasks active: {qc_status}")
        print(f"  Allocated to Summary: {summary_cpu} cores")
        print(f"  Reason: {reason}")
        print(f"{'='*80}\n")
        
        return summary_cpu, reason
    
    else:
        # Unknown task type, use conservative allocation
        default_cpu = max(1, effective_cpu // 2)
        reason = f"Unknown task type, conservative allocation ({default_cpu} CPU)"
        print(f"[CPU Allocator] Warning: Unknown task type '{task_type}', using {default_cpu} CPU")
        return default_cpu, reason


def set_joblib_threads(cores: int, task_type: str):
    """
    Set Joblib threading environment variable for parallel processing.
    
    Args:
        cores: Number of CPU cores to allocate
        task_type: 'qc' or 'summary' (for logging)
    """
    os.environ['JOBLIB_MAX_NUM_THREADS'] = str(cores)
    print(f"[CPU Allocator] Set JOBLIB_MAX_NUM_THREADS={cores} for {task_type.upper()} task")


def allocate_cpu_for_task(task_type: str) -> int:
    """
    Main entry point: Allocate CPU cores for a task and configure environment.
    
    Args:
        task_type: 'qc' or 'summary'
    
    Returns:
        int: Number of CPU cores allocated
    """
    cores, reason = get_cpu_allocation_for_task(task_type)
    set_joblib_threads(cores, task_type)
    
    # Additional logging for verification
    print(f"[CPU Allocator] ✅ CPU allocation complete:")
    print(f"  - Task type: {task_type.upper()}")
    print(f"  - Allocated cores: {cores}")
    print(f"  - JOBLIB_MAX_NUM_THREADS: {os.environ.get('JOBLIB_MAX_NUM_THREADS')}")
    print(f"  - Decision: {reason}\n")
    
    return cores
