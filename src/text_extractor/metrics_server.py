# metrics_server.py
from prometheus_client import start_http_server, Counter, Gauge, Histogram

# Define metrics (exported names should be stable)
MSG_PROCESSED = Counter("worker_messages_processed_total", "Total messages processed", ["queue"])
MSG_FAILED = Counter("worker_messages_failed_total", "Total failed messages", ["queue"])
MSG_RECEIVED = Counter("worker_messages_received_total", "Total messages received from SQS", ["queue"])
IN_FLIGHT = Gauge("worker_messages_in_flight", "Messages currently being processed")
PROC_TIME = Histogram("worker_message_processing_seconds", "Time spent processing a message", ["queue"])

def start_metrics_server(host="0.0.0.0", port=8000):
    # Prometheus start_http_server binds to host:port
    start_http_server(port, addr=host)
