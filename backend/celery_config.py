"""
Celery configuration for distributed task processing
"""
import os

# Redis configuration
# Priority: REDIS_PUBLIC_URL (for external connections) > REDIS_URL (for internal Railway) > localhost
REDIS_URL = os.getenv("REDIS_PUBLIC_URL") or os.getenv("REDIS_URL") or "redis://localhost:6379/0"

# Fix incorrect port 6380 -> 6379 (standard Redis port)
if ":6380" in REDIS_URL:
    print(f"[WARN] Found incorrect Redis port 6380, correcting to 6379")
    REDIS_URL = REDIS_URL.replace(":6380", ":6379")

# Celery broker settings
broker_url = REDIS_URL
result_backend = REDIS_URL

# Connection retry settings (fixes Railway Redis proxy timeouts)
broker_connection_retry_on_startup = True  # Retry connection on startup (Celery 6+ compat)
broker_connection_retry = True
broker_connection_max_retries = 10
broker_connection_timeout = 30  # seconds

# Transport options - socket-level timeouts for Railway proxy
broker_transport_options = {
    'socket_timeout': 30,
    'socket_connect_timeout': 30,
    'retry_on_timeout': True,
    'socket_keepalive': True,
}

# Disable mingle (neighbor discovery) - this is what causes the timeout crash
worker_enable_remote_control = True
worker_hijack_root_logger = False

# Task settings
task_serializer = "json"
accept_content = ["json"]
result_serializer = "json"
timezone = "UTC"
enable_utc = True

# Task execution settings
task_track_started = True  # Track when task starts
task_time_limit = 60 * 60  # 60 minutes hard limit
task_soft_time_limit = 55 * 60  # 55 minutes soft limit

# Worker settings
worker_prefetch_multiplier = 1  # Prevent task hoarding (queue-based model)
worker_max_tasks_per_child = 1000

# Queue-based routing: separate queues for QC and Summary
task_routes = {
    'tasks.process_qc_new_unified_task': {'queue': 'qc'},
    'tasks.process_ocr_task': {'queue': 'summary'},
    'tasks.process_phase1_task': {'queue': 'summary'},
}

# Rate limiting: Max 1 concurrent task per type
task_annotations = {
    'tasks.process_qc_new_unified_task': {
        'rate_limit': '1/s',
        'time_limit': 3600,  # 60 minutes for QC
    },
    'tasks.process_ocr_task': {
        'rate_limit': '1/s',
    }
}

# Result backend settings
result_expires = 3600  # Results expire after 1 hour
