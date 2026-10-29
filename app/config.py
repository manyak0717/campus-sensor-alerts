"""Central configuration. Kept as plain module-level constants so the whole
app can be tuned from one place without hunting through files.
"""
import os

# Where the SQLite database file lives. In-memory (":memory:") is NOT used
# because we need data to survive a process restart (see ARCHITECTURE.md).
DB_PATH = os.environ.get("SENSOR_DB_PATH", "sensor_alerts.db")

# How many readings the background consumer pulls off the queue and writes
# to the database in one batch. Bigger = fewer, cheaper DB round-trips;
# smaller = readings become visible/alertable sooner. 500 is a reasonable
# middle ground for a campus-scale deployment.
WRITE_BATCH_SIZE = 500

# Max time the consumer waits before flushing a partial batch, so readings
# don't sit unflushed for a long time during a quiet period.
WRITE_FLUSH_INTERVAL_SECONDS = 0.5

# Bounded queue size between ingestion and processing. Bounded (not
# unlimited) so a sustained overload fails loudly (ingestion starts
# rejecting with 503) instead of growing memory without limit until the
# process is killed. See ARCHITECTURE.md "backpressure" section.
INGEST_QUEUE_MAXSIZE = 200_000

# When we rehydrate rolling windows on startup, how far back to look. Must
# be >= the largest window any rule uses, or a just-restarted process will
# under-count a window rule until enough new readings arrive.
MAX_WINDOW_SECONDS = 60 * 60  # 1 hour of lookback on startup

# Minimum time between two alerts for the *same* rule + sensor, so one
# faulty sensor spamming bad readings doesn't flood the alerts list with
# duplicates of the same problem.
ALERT_COOLDOWN_SECONDS = 60
