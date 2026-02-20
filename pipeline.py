import requests
import json
import os
import time
import logging
import sqlite3
import threading
import csv
from datetime import datetime, timedelta
from urllib.parse import urlparse, parse_qs
import zoneinfo
from queue import Queue

from dotenv import load_dotenv
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def load_locations_from_csv(csv_path):
    """Load locations configuration from CSV file"""
    locations_config = {}

    try:
        with open(csv_path, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                location = row['location'].strip()
                interval = int(row['interval'].strip())  # strip whitespace/\r before casting
                priority = row['priority'].strip().lower()

                if priority not in ['high', 'medium', 'low']:
                    logger.warning(
                        f"Invalid priority '{priority}' for {location}, defaulting to 'medium'"
                    )
                    priority = 'medium'

                locations_config[location] = {
                    'interval': interval,
                    'priority': priority
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
    """Load locations configuration from JSON file"""
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

            # FIX: consistent with CSV loader — warn and default instead of hard raise
            if config['priority'].lower() not in ['high', 'medium', 'low']:
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


class SafeSQLiteResearchETL:
    def __init__(
        self,
        api_url,
        api_token,
        locations_config,
        start_hour=5,
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

        # FIX: queue sized to absorb ~2.5 hrs of backpressure at 18k records/hr
        self.write_queue = Queue(maxsize=50_000)

        self.records_processed = 0
        self._records_lock = threading.Lock()

        # FIX: last_polls protected by its own lock — concurrent poll threads
        # both read and write this dict, so a plain dict is a race condition
        self.last_polls = {}
        self._last_polls_lock = threading.Lock()

        self.last_cleanup = datetime.now()
        self.running = False

        # Rate-limited stats logging
        self._last_stats_log = time.monotonic()
        self._stats_log_interval = 60  # seconds

        self.headers = {
            'User-Agent': 'Research-ETL-Pipeline/1.0',
            token_header: api_token,
        }
        # Token lives only in headers dict, not as a separate instance attribute

        self.setup_database()

    # ------------------------------------------------------------------
    # Database
    # ------------------------------------------------------------------

    def setup_database(self):
        """Create database with WAL mode and incremental auto-vacuum"""
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)

        conn = sqlite3.connect(self.db_path, timeout=30)
        cursor = conn.cursor()

        cursor.execute('PRAGMA journal_mode=WAL')
        cursor.execute('PRAGMA busy_timeout=5000')

        # FIX: incremental auto-vacuum instead of manual VACUUM calls.
        # Must be set before any tables are created (or on an empty DB).
        # This avoids full-DB rewrites that lock reads/writes for seconds
        # to minutes on a 1.3M-row database.
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
        """Delete records older than retention_days, then incrementally reclaim space"""
        try:
            conn = sqlite3.connect(self.db_path, timeout=30)
            cursor = conn.cursor()

            # FIX: cutoff must use the same timezone as sampled_at.
            # SQLite compares timestamps as strings lexicographically, so both
            # sides must use the same offset format (+08:00 for SGT).
            tz = zoneinfo.ZoneInfo(self.timezone)
            cutoff = datetime.now(tz) - timedelta(days=self.retention_days)
            cutoff_str_raw = cutoff.strftime('%Y-%m-%dT%H:%M:%S%z')
            # %z produces +0800, ISO 8601 needs +08:00
            cutoff_str = cutoff_str_raw[:-2] + ':' + cutoff_str_raw[-2:]

            logger.info(f"Cleanup: removing records before {cutoff_str}")

            cursor.execute(
                'DELETE FROM samples WHERE sampled_at < ?', (cutoff_str,)
            )
            deleted = cursor.rowcount
            conn.commit()

            if deleted > 0:
                logger.info(f"Deleted {deleted} old records")
                # FIX: incremental vacuum — frees pages in small batches without
                # locking the entire database file the way VACUUM does
                cursor.execute('PRAGMA incremental_vacuum')
                conn.commit()
                logger.info("Incremental vacuum complete")
            else:
                logger.info("No records to delete")

            conn.close()

        except sqlite3.OperationalError as e:
            logger.error(f"Cleanup error: {e}")

    # ------------------------------------------------------------------
    # Scheduling helpers
    # ------------------------------------------------------------------

    def is_active_hours(self):
        """Check if current time is within active hours in the configured timezone.

        FIX: always use self.timezone (Asia/Singapore), not machine local time.
        Fly.io machines run UTC by default — without this, start_hour/end_hour
        are interpreted as UTC and would be wrong by 8 hours for SGT.
        """
        now = datetime.now(zoneinfo.ZoneInfo(self.timezone))
        return self.start_hour <= now.hour < self.end_hour

    def should_poll_location(self, location):
        """
        Thread-safe check-and-claim for a location poll slot.

        FIX: the check and the update of last_polls must happen atomically
        inside the lock. Without this, two concurrent poll threads can both
        pass the interval check for the same location and fire duplicate
        requests before either has updated last_polls.
        """
        poll_interval = self.locations_config[location]['interval']
        with self._last_polls_lock:
            last_poll = self.last_polls.get(location, datetime.min)
            if (datetime.now() - last_poll).total_seconds() >= poll_interval:
                self.last_polls[location] = datetime.now()  # claim immediately
                return True
            return False

    # ------------------------------------------------------------------
    # Polling — separate threads per priority group
    # ------------------------------------------------------------------

    def extract_and_queue(self):
        """
        FIX: split locations into two independent polling threads by priority.

        Original design: single loop sweeping all 2000 locations with 0.5s
        stagger = 1000s per sweep. 5-minute locations need a poll every 300s,
        so the single-loop design was structurally unable to meet its own
        intervals.

        New design:
          - High-priority (5-min) group: 0.25s stagger → 250s sweep ✓
          - Low-priority  (10-min) group: 0.5s stagger  → 500s sweep ✓

        Each group runs in its own daemon thread so neither blocks the other.
        The main thread just waits for both to finish (they won't unless
        self.running is cleared).
        """
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
            f"Poll groups — high: {len(high_group)} locations @ 0.1s stagger, "
            f"low/medium: {len(low_group)} locations @ {self.stagger_interval}s stagger"
        )

        high_thread = threading.Thread(
            target=self._poll_group,
            args=(high_group, 0.1),
            name='poller-high',
            daemon=True,
        )
        low_thread = threading.Thread(
            target=self._poll_group,
            args=(low_group, self.stagger_interval),
            name='poller-low',
            daemon=True,
        )

        high_thread.start()
        low_thread.start()

        # Periodic cleanup (hourly) and stats logging on the main extract thread
        while self.running:
            if (datetime.now() - self.last_cleanup).total_seconds() >= 3600:
                self.cleanup_old_data()
                self.last_cleanup = datetime.now()

            self._maybe_log_stats()
            time.sleep(10)

        high_thread.join()
        low_thread.join()

    def _poll_group(self, locations, stagger):
        """Poll a subset of locations on a fixed stagger interval"""
        location_list = list(locations.keys())

        while self.running:
            if not self.is_active_hours():
                time.sleep(60)
                continue

            # Check and poll each location individually rather than batching
            # the should_poll_location checks. This ensures the interval timing
            # is accurate for all locations regardless of where they appear in the list.
            polled_this_cycle = 0
            for location in location_list:
                if not self.running:
                    break
                
                if self.should_poll_location(location):
                    self._fetch_and_queue(location)
                    polled_this_cycle += 1
                    time.sleep(stagger)
            
            # If no locations were ready, sleep before checking again
            if polled_this_cycle == 0:
                time.sleep(1)

    def _fetch_and_queue(self, location):
        """Fetch one location from the API and push to the write queue"""
        try:
            response = requests.get(
                self.api_url,
                params={self.location_param: location},
                headers=self.headers,
                timeout=15,
            )
            response.raise_for_status()

            data = response.json()

            with self._records_lock:
                self.records_processed += 1

            priority = self.locations_config[location]['priority']

            # FIX: store sampled_at in configured timezone (SGT) so timestamps
            # are human-readable in local time without conversion. Use %z to
            # derive offset from the timezone object, then format as ISO 8601.
            tz = zoneinfo.ZoneInfo(self.timezone)
            ts = datetime.now(tz).strftime('%Y-%m-%dT%H:%M:%S%z')
            # %z produces +0800, ISO 8601 needs +08:00
            ts_iso = ts[:-2] + ':' + ts[-2:]

            record = {
                'location': location,
                'priority': priority,
                'data': json.dumps(data),
                'sampled_at': ts_iso,
            }

            try:
                self.write_queue.put_nowait(record)
            except Exception:
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
    # FIX: rate-limited stats logging
    # Previously logged queue size on every single insert — at 5 records/sec
    # that's 18,000 log lines/hour of noise. Now logs a summary every 60s.
    # ------------------------------------------------------------------

    def _maybe_log_stats(self):
        now = time.monotonic()
        if now - self._last_stats_log >= self._stats_log_interval:
            logger.info(
                f"[stats] processed={self.records_processed} "
                f"queue={self.write_queue.qsize()}/{self.write_queue.maxsize}"
            )
            self._last_stats_log = now

    # ------------------------------------------------------------------
    # Writer thread
    # ------------------------------------------------------------------

    def batch_write_to_db(self, batch_size=500, max_wait_seconds=2):
        """
        FIX: dynamic batching — commit when we hit batch_size OR max_wait_seconds,
        whichever comes first.

        Original: hardcoded batch of 50 with a 5s timeout, resulting in small
        commits that don't amortise SQLite's per-commit overhead well at high
        throughput. New defaults (500 records or 2s) balance latency and
        write efficiency for 18k records/hr.
        """
        while self.running or not self.write_queue.empty():
            batch = []
            deadline = time.monotonic() + max_wait_seconds

            while len(batch) < batch_size and time.monotonic() < deadline:
                try:
                    record = self.write_queue.get(timeout=0.1)
                    batch.append(record)
                except Exception:
                    # Timeout on get — check deadline and loop
                    break

            if batch:
                self._write_batch(batch)
            else:
                time.sleep(0.5)

    def _write_batch(self, batch):
        """Write a batch of records to SQLite, spilling to overflow on failure"""
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

            logger.debug(f"Wrote {len(batch)} records")

        except sqlite3.OperationalError as e:
            logger.error(f"DB write error ({e}), spilling {len(batch)} records to overflow file")
            self._spill_to_overflow(batch)

    def _spill_to_overflow(self, batch):
        """
        FIX: on DB failure, write undelivered records to a line-delimited JSON
        file rather than attempting put_nowait into a potentially full queue
        and silently dropping them.

        A separate replay mechanism (not implemented here) can re-ingest these
        once the DB recovers.
        """
        overflow_path = self.db_path + '.overflow.jsonl'
        try:
            with open(overflow_path, 'a') as f:
                for record in batch:
                    f.write(json.dumps(record) + '\n')
            logger.warning(
                f"Spilled {len(batch)} records to {overflow_path}"
            )
        except OSError as e:
            logger.error(f"Could not write overflow file: {e} — {len(batch)} records lost")

    # ------------------------------------------------------------------
    # Query interface
    # ------------------------------------------------------------------

    def get_data(self, location=None, limit=100, offset=0, date_from=None, date_to=None):
        """Query the samples table with optional filters"""
        try:
            conn = sqlite3.connect(self.db_path, timeout=30)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()

            conditions = []
            args = []

            if location:
                conditions.append('location = ?')
                args.append(location)
            if date_from:
                conditions.append('sampled_at >= ?')
                # Use %z to get the offset from configured timezone
                tz = zoneinfo.ZoneInfo(self.timezone)
                ts = datetime.strptime(f'{date_from}T00:00:00', '%Y-%m-%dT%H:%M:%S').replace(tzinfo=tz)
                ts_str = ts.strftime('%Y-%m-%dT%H:%M:%S%z')
                args.append(ts_str[:-2] + ':' + ts_str[-2:])
            if date_to:
                conditions.append('sampled_at <= ?')
                tz = zoneinfo.ZoneInfo(self.timezone)
                ts = datetime.strptime(f'{date_to}T23:59:59', '%Y-%m-%dT%H:%M:%S').replace(tzinfo=tz)
                ts_str = ts.strftime('%Y-%m-%dT%H:%M:%S%z')
                args.append(ts_str[:-2] + ':' + ts_str[-2:])

            where = ('WHERE ' + ' AND '.join(conditions)) if conditions else ''
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

    def get_stats(self):
        """Return pipeline and database statistics"""
        try:
            conn = sqlite3.connect(self.db_path, timeout=30)
            cursor = conn.cursor()

            cursor.execute('SELECT COUNT(*) FROM samples')
            total = cursor.fetchone()[0]

            cursor.execute('''
                SELECT location,
                       COUNT(*)       AS count,
                       MIN(sampled_at) AS first_sample,
                       MAX(sampled_at) AS last_sample
                FROM samples
                GROUP BY location
            ''')
            by_location = {
                row[0]: {
                    'count': row[1],
                    'first_sample': row[2],
                    'last_sample': row[3],
                }
                for row in cursor.fetchall()
            }

            cursor.execute('SELECT MIN(sampled_at), MAX(sampled_at) FROM samples')
            first_update, last_update = cursor.fetchone()

            db_size_mb = (
                os.path.getsize(self.db_path) / (1024 * 1024)
                if os.path.exists(self.db_path)
                else 0.0
            )

            conn.close()

            return {
                'total_records': total,
                'by_location': by_location,
                'first_sampled_at': first_update,
                'last_sampled_at': last_update,
                'queue_size': self.write_queue.qsize(),
                'queue_max': self.write_queue.maxsize,
                'records_processed': self.records_processed,
                'db_size_mb': round(db_size_mb, 2),
                'retention_days': self.retention_days,
                'stagger_interval': self.stagger_interval,
            }

        except sqlite3.OperationalError as e:
            logger.error(f"DB stats error: {e}")
            return {}

    # ------------------------------------------------------------------
    # Entry point
    # ------------------------------------------------------------------

    def run(self):
        """Start writer thread then run the extract loop"""
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
            kwargs={'batch_size': 500, 'max_wait_seconds': 2},
            name='writer',
            daemon=False,   # keep alive until queue drains on shutdown
        )
        writer_thread.start()

        try:
            self.extract_and_queue()
        except KeyboardInterrupt:
            logger.info("Interrupt received — shutting down pollers")
        except Exception as e:
            logger.error(f"Unexpected error in extract loop: {e}", exc_info=True)
        finally:
            # FIX: always clear running so the writer thread can exit cleanly,
            # even if an unexpected exception escapes extract_and_queue
            self.running = False
            logger.info("Waiting for writer thread to flush remaining records...")
            writer_thread.join(timeout=30)
            logger.info("Shutdown complete")


# ------------------------------------------------------------------
# HTTP server
# ------------------------------------------------------------------

def start_http_server(pipeline):
    """Expose pipeline data via a simple HTTP API"""
    # FIX: ThreadingHTTPServer so a slow /export query doesn't block /stats
    from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler
    import re

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            parsed = urlparse(self.path)
            path = parsed.path
            params = parse_qs(parsed.query)

            def get_param(key, default=None, cast=str):
                val = params.get(key, [None])[0]
                if val is None:
                    return default
                try:
                    return cast(val)
                except (ValueError, TypeError):
                    # FIX: return 400 on bad cast rather than silently using default
                    self._send_json(
                        {'error': f"Invalid value for '{key}': '{val}'"},
                        status=400,
                    )
                    return None

            def parse_dates():
                date_from = get_param('date_from')
                date_to = get_param('date_to')
                if date_from is None and 'date_from' in params:
                    return None   # bad cast already sent 400
                if date_to is None and 'date_to' in params:
                    return None
                for label, val in [('date_from', date_from), ('date_to', date_to)]:
                    if val:
                        try:
                            datetime.strptime(val, '%Y-%m-%d')
                        except ValueError:
                            self._send_json(
                                {'error': f"Invalid {label} '{val}', expected YYYY-MM-DD"},
                                status=400,
                            )
                            return None
                return date_from, date_to

            def send_data(data, filename=None):
                body = json.dumps(data, indent=2 if filename else None).encode()
                self.send_response(200)
                self.send_header('Content-Type', 'application/json')
                if filename:
                    self.send_header(
                        'Content-Disposition', f'attachment; filename="{filename}"'
                    )
                self.end_headers()
                self.wfile.write(body)

            MAX_LIMIT_DATA = 10_000
            MAX_LIMIT_EXPORT = 100_000

            if path == '/data':
                location = get_param('location')
                limit_raw = get_param('limit', 100, int)
                if limit_raw is None:
                    return
                limit = min(limit_raw, MAX_LIMIT_DATA)
                offset_raw = get_param('offset', 0, int)
                if offset_raw is None:
                    return
                dates = parse_dates()
                if dates is None:
                    return
                date_from, date_to = dates

                data = pipeline.get_data(
                    location=location, limit=limit, offset=offset_raw,
                    date_from=date_from, date_to=date_to,
                )
                send_data(data)

            elif path == '/stats':
                send_data(pipeline.get_stats())

            elif path == '/export':
                location = get_param('location')
                limit_raw = get_param('limit', 10_000, int)
                if limit_raw is None:
                    return
                limit = min(limit_raw, MAX_LIMIT_EXPORT)
                offset_raw = get_param('offset', 0, int)
                if offset_raw is None:
                    return
                dates = parse_dates()
                if dates is None:
                    return
                date_from, date_to = dates

                data = pipeline.get_data(
                    location=location, limit=limit, offset=offset_raw,
                    date_from=date_from, date_to=date_to,
                )
                safe_loc = re.sub(r'[^\w\-]', '_', location) if location else 'all'
                send_data(data, filename=f"research_{safe_loc}.json")

            else:
                self.send_response(404)
                self.end_headers()

        def _send_json(self, body, status=200):
            encoded = json.dumps(body).encode()
            self.send_response(status)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            self.wfile.write(encoded)

        def log_message(self, format, *args):
            pass  # suppress default per-request stderr logging

    server = ThreadingHTTPServer(('0.0.0.0', 8080), Handler)
    logger.info("HTTP server started on port 8080 (threaded)")
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

    http_thread = threading.Thread(
        target=start_http_server, args=(pipeline,), daemon=True
    )
    http_thread.start()

    pipeline.run()
    # No infinite loop needed — pipeline.run() only returns after clean shutdown.
    # The HTTP server is daemon=True so it exits with the process.