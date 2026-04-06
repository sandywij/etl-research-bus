import requests
import requests.adapters
import json
import os
import time
import logging
import sqlite3
import threading
import csv
import heapq
import concurrent.futures
import queue as queue_module
import re
import io
from datetime import datetime, timedelta
from urllib.parse import urlparse, parse_qs
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler
import zoneinfo
from queue import Queue

from dotenv import load_dotenv
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# ------------------------------------------------------------------
# Utility
# ------------------------------------------------------------------

def _iso8601_tz(ts: str) -> str:
    """Convert a strftime %z suffix (+0800) to ISO 8601 colon form (+08:00)."""
    return ts[:-2] + ':' + ts[-2:]


def _now_iso(timezone: str) -> str:
    """Return the current time in the given timezone as an ISO 8601 string."""
    tz = zoneinfo.ZoneInfo(timezone)
    ts = datetime.now(tz).strftime('%Y-%m-%dT%H:%M:%S%z')
    return _iso8601_tz(ts)


# ------------------------------------------------------------------
# Location loaders
# ------------------------------------------------------------------

def load_locations_from_csv(csv_path):
    """Load locations configuration from CSV file."""
    locations_config = {}

    try:
        with open(csv_path, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                location = row['location'].strip()
                interval = int(row['interval'].strip())
                priority = row['priority'].strip().lower()

                if priority not in ('high', 'medium', 'low'):
                    logger.warning(
                        f"Invalid priority '{priority}' for {location}, defaulting to 'medium'"
                    )
                    priority = 'medium'

                locations_config[location] = {
                    'interval': interval,
                    'priority': priority,
                }

        logger.info(f"Loaded {len(locations_config)} locations from {csv_path}")
        return locations_config

    except FileNotFoundError:
        logger.error(f"CSV file not found: {csv_path}")
        raise
    except KeyError as e:
        logger.error(f"CSV missing required column: {e}")
        raise
    except ValueError as e:
        logger.error(f"Invalid data in CSV: {e}")
        raise


def load_locations_from_json(json_path):
    """Load locations configuration from JSON file."""
    try:
        with open(json_path, 'r') as f:
            locations_config = json.load(f)

        for location, config in locations_config.items():
            if 'interval' not in config or 'priority' not in config:
                raise ValueError(
                    f"Location {location} missing 'interval' or 'priority'"
                )

            if not isinstance(config['interval'], int):
                raise ValueError(
                    f"Location {location} interval must be integer (seconds)"
                )

            if config['priority'].lower() not in ('high', 'medium', 'low'):
                logger.warning(
                    f"Invalid priority '{config['priority']}' for {location}, defaulting to 'medium'"
                )
                config['priority'] = 'medium'

        logger.info(f"Loaded {len(locations_config)} locations from {json_path}")
        return locations_config

    except FileNotFoundError:
        logger.error(f"JSON file not found: {json_path}")
        raise
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in {json_path}: {e}")
        raise


# ------------------------------------------------------------------
# Pipeline
# ------------------------------------------------------------------

class SafeSQLiteResearchETL:
    def __init__(
        self,
        api_url,
        api_token,
        locations_config,
        start_hour=6,
        end_hour=24,
        db_path='/data/research.db',
        retention_days=3,
        token_header='AccountKey',
        location_param='BusStopCode',
        stagger_interval=0.5,
        timezone='Asia/Singapore',
    ):
        self.api_url = api_url
        self.locations_config = locations_config
        self.start_hour = start_hour
        self.end_hour = end_hour
        self.db_path = db_path
        self.retention_days = retention_days
        self.token_header = token_header
        self.location_param = location_param
        self.stagger_interval = stagger_interval
        self.timezone = timezone

        self.write_queue = Queue(maxsize=100_000)

        self._api_fetches = 0
        self._records_queued = 0
        self._records_written = 0
        self._records_lock = threading.Lock()

        self.last_cleanup = datetime.now()
        self.running = False

        self._last_stats_log = time.monotonic()
        self._stats_log_interval = 60  # seconds

        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Research-ETL-Pipeline/1.0',
            token_header: api_token,
        })
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=1,       # single host (LTA API)
            pool_maxsize=16,          # > 12 worker threads
            max_retries=0,            # errors handled in _fetch_and_queue
        )
        self.session.mount('https://', adapter)
        self.session.mount('http://', adapter)

        self.setup_database()

    # ------------------------------------------------------------------
    # Database
    # ------------------------------------------------------------------

    def setup_database(self):
        """Create database with WAL mode and incremental auto-vacuum."""
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)

        conn = sqlite3.connect(self.db_path, timeout=30)
        cursor = conn.cursor()

        cursor.execute('PRAGMA journal_mode=WAL')
        cursor.execute('PRAGMA busy_timeout=15000')
        cursor.execute('PRAGMA auto_vacuum=INCREMENTAL')

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS samples (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                location   TEXT      NOT NULL,
                data       TEXT      NOT NULL,
                sampled_at TIMESTAMP NOT NULL,
                priority   TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_location_sampled
            ON samples(location, sampled_at)
        ''')

        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_sampled_at
            ON samples(sampled_at)
        ''')

        conn.commit()
        conn.close()
        logger.info(f"Database initialised at {self.db_path}")

    def cleanup_old_data(self):
        """
        Delete records older than retention_days in small batches so the DB
        lock is never held long enough to block the writer thread.
        """
        tz = zoneinfo.ZoneInfo(self.timezone)
        cutoff = datetime.now(tz) - timedelta(days=self.retention_days)
        cutoff_str = _iso8601_tz(cutoff.strftime('%Y-%m-%dT%H:%M:%S%z'))
        logger.info(f"Cleanup: removing records before {cutoff_str}")

        total_deleted = 0
        batch_size = 1_000

        try:
            while True:
                conn = sqlite3.connect(self.db_path, timeout=30)
                cursor = conn.cursor()
                cursor.execute(
                    "DELETE FROM samples WHERE id IN "
                    "(SELECT id FROM samples WHERE sampled_at < ? LIMIT ?)",
                    (cutoff_str, batch_size)
                )
                deleted = cursor.rowcount
                conn.commit()
                conn.close()
                total_deleted += deleted
                if deleted < batch_size:
                    break
                time.sleep(0.1)

            if total_deleted > 0:
                logger.info(f"Deleted {total_deleted} old records (batched)")
                conn = sqlite3.connect(self.db_path, timeout=30)
                conn.execute('PRAGMA incremental_vacuum')
                conn.commit()
                conn.close()
                logger.info("Incremental vacuum complete")
            else:
                logger.info("No records to delete")

        except sqlite3.OperationalError as e:
            logger.error(f"Cleanup error after {total_deleted} deleted: {e}")

    # ------------------------------------------------------------------
    # Scheduling helpers
    # ------------------------------------------------------------------

    def is_active_hours(self):
        """Check if current time is within active hours in the configured timezone."""
        now = datetime.now(zoneinfo.ZoneInfo(self.timezone))
        return self.start_hour <= now.hour < self.end_hour

    def _build_schedule_heap(self, locations: dict) -> list:
        """Build an initial min-heap of (next_poll_monotonic, location) entries."""
        heap = []
        location_list = list(locations.keys())
        n = len(location_list)

        for i, location in enumerate(location_list):
            interval = self.locations_config[location]['interval']
            offset = (i / max(n, 1)) * interval
            next_poll = time.monotonic() + offset
            heapq.heappush(heap, (next_poll, location))

        return heap

    # ------------------------------------------------------------------
    # Polling — separate threads per priority group
    # ------------------------------------------------------------------

    def extract_and_queue(self):
        """Split locations into two independent polling threads by priority."""
        high_stagger = float(os.getenv('HIGH_STAGGER', '0.05'))
        low_stagger = self.stagger_interval
        high_workers = int(os.getenv('HIGH_WORKERS', '8'))
        low_workers = int(os.getenv('LOW_WORKERS', '4'))

        high_group = {
            loc: cfg
            for loc, cfg in self.locations_config.items()
            if cfg['priority'] == 'high'
        }
        low_group = {
            loc: cfg
            for loc, cfg in self.locations_config.items()
            if cfg['priority'] in ('medium', 'low')
        }

        logger.info(
            f"Poll groups — high: {len(high_group)} locations @ {high_stagger}s stagger "
            f"x {high_workers} workers, "
            f"low/medium: {len(low_group)} locations @ {low_stagger}s stagger "
            f"x {low_workers} workers"
        )

        high_thread = threading.Thread(
            target=self._poll_group,
            args=(high_group, high_stagger, high_workers),
            name='poller-high',
            daemon=True,
        )
        low_thread = threading.Thread(
            target=self._poll_group,
            args=(low_group, low_stagger, low_workers),
            name='poller-low',
            daemon=True,
        )

        high_thread.start()
        low_thread.start()

        while self.running:
            if (datetime.now() - self.last_cleanup).total_seconds() >= 3600:
                self.cleanup_old_data()
                self.last_cleanup = datetime.now()

            self._maybe_log_stats()
            time.sleep(10)

        high_thread.join()
        low_thread.join()

    def _poll_group(self, locations: dict, stagger: float, num_workers: int):
        """Drive a group of locations using a min-heap scheduler feeding a ThreadPoolExecutor."""
        if not locations:
            logger.info(f"[{threading.current_thread().name}] No locations assigned, exiting.")
            return

        heap = self._build_schedule_heap(locations)
        heap_lock = threading.Lock()
        thread_name = threading.current_thread().name
        was_inactive = False

        def fetch_and_reschedule(location, intended_fire_time):
            self._fetch_and_queue(location)
            interval = self.locations_config[location]['interval']
            ideal_next = intended_fire_time + interval
            clamped_next = max(ideal_next, time.monotonic())
            with heap_lock:
                heapq.heappush(heap, (clamped_next, location))

        while self.running:
            with concurrent.futures.ThreadPoolExecutor(
                max_workers=num_workers,
                thread_name_prefix=thread_name,
            ) as pool:
                while self.running:
                    if not self.is_active_hours():
                        if not was_inactive:
                            logger.info(f"[{thread_name}] Outside active hours — pausing")
                            was_inactive = True
                        time.sleep(60)
                        continue

                    if was_inactive:
                        logger.info(
                            f"[{thread_name}] Active hours resumed — "
                            f"rescheduling {len(heap)} locations from now"
                        )
                        heap = self._build_schedule_heap(locations)
                        heap_lock = threading.Lock()
                        was_inactive = False
                        break

                    with heap_lock:
                        if heap:
                            next_poll, location = heap[0]
                            wait = next_poll - time.monotonic()
                            if wait <= 0:
                                heapq.heappop(heap)
                                pool.submit(fetch_and_reschedule, location, next_poll)
                                continue

                    with heap_lock:
                        wait = (heap[0][0] - time.monotonic()) if heap else 1
                    time.sleep(min(max(wait, 0), 1))

    def _fetch_and_queue(self, location):
        """Fetch one location from the API and push to the write queue."""
        try:
            response = self.session.get(
                self.api_url,
                params={self.location_param: location},
                timeout=15,
            )
            response.raise_for_status()

            data = response.json()
            priority = self.locations_config[location]['priority']
            sampled_at = _now_iso(self.timezone)

            record = {
                'location': location,
                'priority': priority,
                'data': json.dumps(data),
                'sampled_at': sampled_at,
            }

            with self._records_lock:
                self._api_fetches += 1

            try:
                self.write_queue.put(record, timeout=5)
                with self._records_lock:
                    self._records_queued += 1
            except queue_module.Full:
                logger.warning(
                    f"Queue full ({self.write_queue.qsize()}), dropping {location}"
                )

        except requests.exceptions.HTTPError as e:
            status = e.response.status_code
            if status == 401:
                logger.error(f"Auth failed (401) for {location} — check API token")
            elif status == 403:
                logger.error(f"Permission denied (403) for {location}")
            elif status == 429:
                logger.warning(
                    f"Rate limited (429) for {location} — consider increasing stagger"
                )
            else:
                logger.warning(f"HTTP {status} for {location}: {e}")

        except requests.exceptions.RequestException as e:
            logger.warning(f"Request failed for {location}: {e}")

    # ------------------------------------------------------------------
    # Stats logging
    # ------------------------------------------------------------------

    def _maybe_log_stats(self):
        now = time.monotonic()
        if now - self._last_stats_log >= self._stats_log_interval:
            with self._records_lock:
                fetches = self._api_fetches
                queued = self._records_queued
                written = self._records_written
            logger.info(
                f"[stats] api_fetches={fetches} queued={queued} written={written} "
                f"queue={self.write_queue.qsize()}/{self.write_queue.maxsize}"
            )
            self._last_stats_log = now

    # ------------------------------------------------------------------
    # Writer thread
    # ------------------------------------------------------------------

    def batch_write_to_db(self, batch_size=1000, max_wait_seconds=5):
        """Dynamic batching — commit when we hit batch_size OR max_wait_seconds."""
        while self.running or not self.write_queue.empty():
            batch = []
            deadline = time.monotonic() + max_wait_seconds

            while len(batch) < batch_size:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                try:
                    record = self.write_queue.get(timeout=min(0.1, remaining))
                    batch.append(record)
                except queue_module.Empty:
                    continue

            if batch:
                self._write_batch(batch)
            else:
                time.sleep(0.5)

    def _write_batch(self, batch):
        """Write a batch of records to SQLite, spilling to overflow on failure."""
        try:
            conn = sqlite3.connect(self.db_path, timeout=30)
            cursor = conn.cursor()

            cursor.executemany(
                'INSERT INTO samples (location, data, sampled_at, priority) VALUES (?, ?, ?, ?)',
                [
                    (r['location'], r['data'], r['sampled_at'], r['priority'])
                    for r in batch
                ],
            )

            conn.commit()
            conn.close()

            with self._records_lock:
                self._records_written += len(batch)

            logger.debug(f"Wrote {len(batch)} records")

        except sqlite3.OperationalError as e:
            logger.error(f"DB write error ({e}), spilling {len(batch)} records to overflow file")
            self._spill_to_overflow(batch)

    def _spill_to_overflow(self, batch):
        """On DB failure, write undelivered records to a line-delimited JSON file."""
        overflow_path = self.db_path + '.overflow.jsonl'
        try:
            with open(overflow_path, 'a') as f:
                for record in batch:
                    f.write(json.dumps(record) + '\n')
            logger.warning(f"Spilled {len(batch)} records to {overflow_path}")
        except OSError as e:
            logger.error(f"Could not write overflow file: {e} — {len(batch)} records lost")

    # ------------------------------------------------------------------
    # Query interface
    # ------------------------------------------------------------------

    def _build_query(self, location, date_from, date_to):
        """Build the WHERE clause and args list shared by get_data and iter_data."""
        conditions = []
        args = []

        if location:
            conditions.append('location = ?')
            args.append(location)
        if date_from:
            conditions.append('sampled_at >= ?')
            tz = zoneinfo.ZoneInfo(self.timezone)
            ts = datetime.strptime(f'{date_from}T00:00:00', '%Y-%m-%dT%H:%M:%S').replace(tzinfo=tz)
            args.append(_iso8601_tz(ts.strftime('%Y-%m-%dT%H:%M:%S%z')))
        if date_to:
            conditions.append('sampled_at <= ?')
            tz = zoneinfo.ZoneInfo(self.timezone)
            ts = datetime.strptime(f'{date_to}T23:59:59', '%Y-%m-%dT%H:%M:%S').replace(tzinfo=tz)
            args.append(_iso8601_tz(ts.strftime('%Y-%m-%dT%H:%M:%S%z')))

        where = ('WHERE ' + ' AND '.join(conditions)) if conditions else ''
        return where, args

    def get_data(self, location=None, limit=100, offset=0, date_from=None, date_to=None):
        """Query the samples table with optional filters."""
        try:
            conn = sqlite3.connect(self.db_path, timeout=60)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()

            where, args = self._build_query(location, date_from, date_to)
            args += [limit, offset]

            cursor.execute(
                f'SELECT * FROM samples {where} ORDER BY sampled_at DESC LIMIT ? OFFSET ?',
                args,
            )

            rows = cursor.fetchall()
            conn.close()
            return [dict(row) for row in rows]

        except sqlite3.OperationalError as e:
            logger.error(f"DB read error: {e}")
            return []

    def iter_data(self, location=None, limit=100_000, offset=0, date_from=None, date_to=None, chunk_size=5000):
        """
        Generator that yields rows one dict at a time, fetching in chunks.
        Uses PRAGMA query_only=ON so reads never block the WAL writer.
        """
        conn = None
        try:
            conn = sqlite3.connect(self.db_path, timeout=60)
            conn.execute('PRAGMA query_only=ON')
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute('PRAGMA mmap_size=419430400')
            cursor.execute('PRAGMA cache_size=-65536')
            cursor.execute('PRAGMA temp_store=MEMORY')
            cursor.arraysize = chunk_size

            where, args = self._build_query(location, date_from, date_to)
            args += [limit, offset]

            cursor.execute(
                f'SELECT * FROM samples {where} ORDER BY sampled_at DESC LIMIT ? OFFSET ?',
                args,
            )

            while True:
                chunk = cursor.fetchmany()
                if not chunk:
                    break
                for row in chunk:
                    yield dict(row)

        except sqlite3.OperationalError as e:
            logger.error(f"DB read error during export: {e}")
        finally:
            if conn:
                conn.close()

    def get_stats(self):
        """Return pipeline and database statistics."""
        try:
            conn = sqlite3.connect(self.db_path, timeout=30)
            cursor = conn.cursor()

            cursor.execute('SELECT COUNT(*) FROM samples')
            total = cursor.fetchone()[0]

            cursor.execute('SELECT MIN(sampled_at), MAX(sampled_at) FROM samples')
            first_update, last_update = cursor.fetchone()

            cursor.execute('SELECT COUNT(DISTINCT location) FROM samples')
            location_count = cursor.fetchone()[0]

            db_size_mb = (
                os.path.getsize(self.db_path) / (1024 * 1024)
                if os.path.exists(self.db_path)
                else 0.0
            )

            conn.close()

            with self._records_lock:
                api_fetches = self._api_fetches
                records_queued = self._records_queued
                records_written = self._records_written

            return {
                'total_records': total,
                'distinct_locations': location_count,
                'first_sampled_at': first_update,
                'last_sampled_at': last_update,
                'queue_size': self.write_queue.qsize(),
                'queue_max': self.write_queue.maxsize,
                'api_fetches': api_fetches,
                'records_queued': records_queued,
                'records_written': records_written,
                'db_size_mb': round(db_size_mb, 2),
                'retention_days': self.retention_days,
                'stagger_interval': self.stagger_interval,
            }

        except sqlite3.OperationalError as e:
            logger.error(f"DB stats error: {e}")
            return {}

    def get_location_stats(self):
        """Return per-location record counts and sample times."""
        try:
            conn = sqlite3.connect(self.db_path, timeout=60)
            cursor = conn.cursor()

            cursor.execute('''
                SELECT location,
                       COUNT(*)        AS count,
                       MIN(sampled_at) AS first_sample,
                       MAX(sampled_at) AS last_sample
                FROM samples
                GROUP BY location
                ORDER BY location
            ''')

            result = {
                row[0]: {
                    'count': row[1],
                    'first_sample': row[2],
                    'last_sample': row[3],
                }
                for row in cursor.fetchall()
            }

            conn.close()
            return result

        except sqlite3.OperationalError as e:
            logger.error(f"DB location stats error: {e}")
            return {}

    # ------------------------------------------------------------------
    # Entry point
    # ------------------------------------------------------------------

    def run(self):
        """Start writer thread then run the extract loop."""
        self.running = True

        logger.info("=== Starting Research ETL Pipeline ===")
        logger.info(f"API URL:          {self.api_url}")
        logger.info(f"Location param:   {self.location_param}")
        logger.info(f"Token header:     {self.token_header}")
        logger.info(f"Database path:    {self.db_path}")
        logger.info(f"Active hours:     {self.start_hour}:00–{self.end_hour}:00")
        logger.info(f"Retention:        {self.retention_days} days")
        logger.info(f"Timezone:         {self.timezone}")
        logger.info(f"Stagger interval: {self.stagger_interval}s")
        logger.info(f"Total locations:  {len(self.locations_config)}")

        active_hours = self.end_hour - self.start_hour
        total_daily = sum(
            (active_hours * 3600) // cfg['interval']
            for cfg in self.locations_config.values()
        )
        logger.info(f"Est. daily API calls: {total_daily:,}")
        logger.info("=== Pipeline ready ===\n")

        writer_thread = threading.Thread(
            target=self.batch_write_to_db,
            kwargs={'batch_size': 1000, 'max_wait_seconds': 5},
            name='writer',
            daemon=False,
        )
        writer_thread.start()

        try:
            self.extract_and_queue()
        except KeyboardInterrupt:
            logger.info("Interrupt received — shutting down pollers")
        except Exception as e:
            logger.error(f"Unexpected error in extract loop: {e}", exc_info=True)
        finally:
            self.running = False
            remaining = self.write_queue.qsize()
            if remaining:
                logger.info(
                    f"Waiting for writer thread to flush {remaining} remaining records "
                    f"(timeout=60s)..."
                )
            else:
                logger.info("Queue empty — waiting for writer thread to exit...")
            writer_thread.join(timeout=60)
            if writer_thread.is_alive():
                logger.warning(
                    "Writer thread did not finish within 60s — some records may not have been written. "
                    "Check the overflow file."
                )
            else:
                logger.info("Shutdown complete")


# ------------------------------------------------------------------
# HTTP servers
# ------------------------------------------------------------------

_SAFE_LOCATION_RE = re.compile(r'^[\w\-]{1,64}$')
_MISSING = object()

# Export chunk size: number of rows serialised into one HTTP chunk.
_EXPORT_ROWS_PER_CHUNK = int(os.getenv('EXPORT_ROWS_PER_CHUNK', '500'))
# Kernel-side write buffer for the export socket (bytes).
_EXPORT_WRITE_BUFFER = int(os.getenv('EXPORT_WRITE_BUFFER', str(256 * 1024)))
# Throttle delay (seconds) between export chunks during active hours.
_EXPORT_ACTIVE_THROTTLE = float(os.getenv('EXPORT_ACTIVE_THROTTLE', '0.05'))

# Limit concurrent exports to 1.
_export_sem = threading.Semaphore(1)


def start_health_server():
    """Dedicated health check server on port 8081, isolated from main API."""

    class HealthHandler(BaseHTTPRequestHandler):
        protocol_version = 'HTTP/1.1'

        def do_GET(self):
            body = b'{"status":"ok"}'
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Content-Length', str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format, *args):
            pass

    port = int(os.getenv('HEALTH_PORT', '8081'))
    server = ThreadingHTTPServer(('0.0.0.0', port), HealthHandler)
    logger.info(f"Health server started on port {port}")
    server.serve_forever()


def start_http_server(pipeline):
    """Expose pipeline data via a simple HTTP API."""

    class Handler(BaseHTTPRequestHandler):
        protocol_version = 'HTTP/1.1'

        def do_GET(self):
            parsed = urlparse(self.path)
            path = parsed.path
            params = parse_qs(parsed.query)

            def get_param(key, default=_MISSING, cast=str):
                raw = params.get(key, [None])[0]
                if raw is None:
                    return default
                try:
                    return cast(raw)
                except (ValueError, TypeError):
                    self._send_json(
                        {'error': f"Invalid value for '{key}': '{raw}'"},
                        status=400,
                    )
                    return _MISSING

            def require_param(key, cast=str, default=_MISSING):
                val = get_param(key, default=default, cast=cast)
                if val is _MISSING:
                    return None, False
                return val, True

            def parse_dates():
                date_from, ok = require_param('date_from', default=None)
                if not ok:
                    return None
                date_to, ok = require_param('date_to', default=None)
                if not ok:
                    return None

                for label, val in (('date_from', date_from), ('date_to', date_to)):
                    if val is not None:
                        try:
                            datetime.strptime(val, '%Y-%m-%d')
                        except ValueError:
                            self._send_json(
                                {'error': f"Invalid {label} '{val}', expected YYYY-MM-DD"},
                                status=400,
                            )
                            return None
                return date_from, date_to

            def validate_location(raw):
                if raw is None:
                    return None, True
                if not _SAFE_LOCATION_RE.match(raw):
                    self._send_json(
                        {'error': f"Invalid location '{raw}': must be 1–64 word characters or hyphens"},
                        status=400,
                    )
                    return None, False
                return raw, True

            def send_data(data, filename=None):
                body = json.dumps(data, indent=2 if filename else None).encode()
                self.send_response(200)
                self.send_header('Content-Type', 'application/json')
                self.send_header('Content-Length', str(len(body)))
                if filename:
                    self.send_header(
                        'Content-Disposition', f'attachment; filename="{filename}"'
                    )
                self.end_headers()
                self.wfile.write(body)

            def stream_export(row_iter, filename):
                """
                Stream a JSON array using chunked transfer encoding.

                Rows are serialised in batches of _EXPORT_ROWS_PER_CHUNK so
                that each HTTP chunk is a large, contiguous bytes object rather
                than one tiny chunk per record.  The socket is wrapped in a
                BufferedWriter (_EXPORT_WRITE_BUFFER bytes) to avoid a syscall
                per chunk-header write.

                During active polling hours, a small sleep is inserted between
                chunks to avoid starving the writer thread.
                """
                self.send_response(200)
                self.send_header('Content-Type', 'application/json')
                self.send_header('Transfer-Encoding', 'chunked')
                self.send_header(
                    'Content-Disposition', f'attachment; filename="{filename}"'
                )
                self.end_headers()

                buf = self.connection.makefile('wb', buffering=_EXPORT_WRITE_BUFFER)

                def write_chunk(data: bytes):
                    buf.write(f'{len(data):X}\r\n'.encode())
                    buf.write(data)
                    buf.write(b'\r\n')

                row_count = 0
                first_row = True
                batch = []

                def flush_batch(batch: list):
                    nonlocal first_row
                    parts = []
                    for record in batch:
                        encoded = json.dumps(record).encode()
                        if first_row:
                            parts.append(b'[' + encoded)
                            first_row = False
                        else:
                            parts.append(b',' + encoded)
                    write_chunk(b''.join(parts))

                try:
                    for row in row_iter:
                        batch.append(row)
                        row_count += 1
                        if len(batch) >= _EXPORT_ROWS_PER_CHUNK:
                            flush_batch(batch)
                            batch = []
                            # Throttle during active hours to avoid starving writes
                            if pipeline.is_active_hours() and _EXPORT_ACTIVE_THROTTLE > 0:
                                time.sleep(_EXPORT_ACTIVE_THROTTLE)

                    if batch:
                        flush_batch(batch)

                    if first_row:
                        write_chunk(b'[')

                    write_chunk(b']')
                    buf.write(b'0\r\n\r\n')
                    buf.flush()

                except BrokenPipeError:
                    logger.warning(
                        f"Export interrupted at row {row_count} (client disconnected) → {filename}"
                    )
                    raise
                else:
                    logger.info(f"Export complete: {row_count} rows → {filename}")
                finally:
                    row_iter.close()

            MAX_LIMIT_DATA = 10_000
            MAX_LIMIT_EXPORT = 100_000

            if path == '/health':
                send_data({'status': 'ok', 'running': pipeline.running})

            elif path == '/data':
                raw_loc, ok = require_param('location', default=None)
                if not ok:
                    return
                location, ok = validate_location(raw_loc)
                if not ok:
                    return
                limit_raw, ok = require_param('limit', cast=int, default=100)
                if not ok:
                    return
                limit = min(limit_raw, MAX_LIMIT_DATA)
                offset, ok = require_param('offset', cast=int, default=0)
                if not ok:
                    return
                dates = parse_dates()
                if dates is None:
                    return
                date_from, date_to = dates

                data = pipeline.get_data(
                    location=location, limit=limit, offset=offset,
                    date_from=date_from, date_to=date_to,
                )
                send_data(data)

            elif path == '/stats':
                send_data(pipeline.get_stats())

            elif path == '/stats/locations':
                send_data(pipeline.get_location_stats())

            elif path == '/export':
                # Limit to one concurrent export
                if not _export_sem.acquire(blocking=False):
                    self._send_json(
                        {'error': 'Export already in progress, try again shortly'},
                        status=429,
                    )
                    return

                try:
                    raw_loc, ok = require_param('location', default=None)
                    if not ok:
                        return
                    location, ok = validate_location(raw_loc)
                    if not ok:
                        return
                    limit_raw, ok = require_param('limit', cast=int, default=10_000)
                    if not ok:
                        return
                    limit = min(limit_raw, MAX_LIMIT_EXPORT)
                    offset, ok = require_param('offset', cast=int, default=0)
                    if not ok:
                        return
                    dates = parse_dates()
                    if dates is None:
                        return
                    date_from, date_to = dates

                    safe_loc = re.sub(r'[^\w\-]', '_', location) if location else 'all'
                    row_iter = pipeline.iter_data(
                        location=location, limit=limit, offset=offset,
                        date_from=date_from, date_to=date_to,
                    )
                    stream_export(row_iter, filename=f"research_{safe_loc}.json")
                finally:
                    _export_sem.release()

            else:
                self.send_response(404)
                self.end_headers()

        def _send_json(self, body, status=200):
            encoded = json.dumps(body).encode()
            self.send_response(status)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Content-Length', str(len(encoded)))
            self.end_headers()
            self.wfile.write(encoded)

        def handle_one_request(self):
            """Swallow BrokenPipeError from clients that disconnect mid-response."""
            try:
                super().handle_one_request()
            except BrokenPipeError:
                pass

        def log_message(self, format, *args):
            pass

    port = int(os.getenv('API_PORT', '8080'))
    server = ThreadingHTTPServer(('0.0.0.0', port), Handler)
    logger.info(f"HTTP API server started on port {port} (threaded)")
    server.serve_forever()


# ------------------------------------------------------------------
# Main
# ------------------------------------------------------------------

if __name__ == "__main__":
    api_url = os.getenv('API_URL')
    api_token = os.getenv('API_TOKEN')
    token_header = os.getenv('API_TOKEN_HEADER', 'AccountKey')
    location_param = os.getenv('API_LOCATION_PARAM', 'BusStopCode')
    retention_days = int(os.getenv('DB_RETENTION_DAYS', '3'))
    db_path = os.getenv('DB_PATH', '/data/research.db')
    stagger_interval = float(os.getenv('STAGGER_INTERVAL', '0.5'))
    timezone = os.getenv('TIMEZONE', 'Asia/Singapore')

    if not api_url:
        logger.error("API_URL environment variable not set")
        exit(1)

    if not api_token:
        logger.error("API_TOKEN environment variable not set")
        exit(1)

    locations_file = os.getenv('LOCATIONS_FILE', 'locations.csv')

    try:
        if locations_file.endswith('.csv'):
            logger.info(f"Loading locations from CSV: {locations_file}")
            locations_config = load_locations_from_csv(locations_file)
        elif locations_file.endswith('.json'):
            logger.info(f"Loading locations from JSON: {locations_file}")
            locations_config = load_locations_from_json(locations_file)
        else:
            raise ValueError(f"Unsupported file format: {locations_file}")

    except Exception as e:
        logger.error(f"Failed to load locations file: {e}")
        exit(1)

    pipeline = SafeSQLiteResearchETL(
        api_url=api_url,
        api_token=api_token,
        locations_config=locations_config,
        retention_days=retention_days,
        token_header=token_header,
        location_param=location_param,
        db_path=db_path,
        stagger_interval=stagger_interval,
        timezone=timezone,
    )

    # Dedicated health server — isolated from API/export traffic
    health_thread = threading.Thread(
        target=start_health_server, daemon=True
    )
    health_thread.start()

    # Main API server
    http_thread = threading.Thread(
        target=start_http_server, args=(pipeline,), daemon=True
    )
    http_thread.start()

    pipeline.run()