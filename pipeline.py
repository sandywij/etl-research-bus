import requests
import json
import os
import time
import logging
import sqlite3
import threading
import csv
from datetime import datetime, timedelta
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
                interval = int(row['interval'])
                priority = row['priority'].strip().lower()
                
                if priority not in ['high', 'medium', 'low']:
                    logger.warning(f"Invalid priority '{priority}' for {location}, defaulting to 'medium'")
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
                raise ValueError(f"Location {location} missing 'interval' or 'priority'")
            
            if not isinstance(config['interval'], int):
                raise ValueError(f"Location {location} interval must be integer (seconds)")
            
            if config['priority'].lower() not in ['high', 'medium', 'low']:
                raise ValueError(f"Location {location} priority must be 'high', 'medium', or 'low'")
        
        logger.info(f"Loaded {len(locations_config)} locations from {json_path}")
        return locations_config
    
    except FileNotFoundError:
        logger.error(f"JSON file not found: {json_path}")
        raise
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in {json_path}: {e}")
        raise

class SafeSQLiteResearchETL:
    def __init__(self, api_url, api_token, locations_config, start_hour=5, end_hour=24, 
                 db_path='/data/research.db', retention_days=7, token_header='AccountKey',
                 location_param='BusStopCode', stagger_interval=0.5):
        self.api_url = api_url
        self.api_token = api_token
        self.locations_config = locations_config
        self.start_hour = start_hour
        self.end_hour = end_hour
        self.db_path = db_path
        self.retention_days = retention_days
        self.token_header = token_header
        self.location_param = location_param
        self.stagger_interval = stagger_interval
        
        self.write_queue = Queue(maxsize=1000)
        self.records_processed = 0
        self.last_polls = {}
        self.running = False
        
        self.headers = {
            'User-Agent': 'Research-ETL-Pipeline/1.0'
        }
        if self.api_token:
            self.headers[self.token_header] = self.api_token
        
        self.setup_database()
    
    def setup_database(self):
        """Create database with WAL mode for safe concurrent access"""
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        
        conn = sqlite3.connect(self.db_path, timeout=30)
        cursor = conn.cursor()
        
        cursor.execute('PRAGMA journal_mode=WAL')
        conn.execute('PRAGMA busy_timeout=5000')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS samples (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                location TEXT NOT NULL,
                data TEXT NOT NULL,
                sampled_at TIMESTAMP NOT NULL,
                priority TEXT,
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
        logger.info(f"Database initialized at {self.db_path}")
    
    def cleanup_old_data(self):
        """Delete records older than retention_days"""
        try:
            conn = sqlite3.connect(self.db_path, timeout=30)
            cursor = conn.cursor()
            
            cutoff_date = datetime.now() - timedelta(days=self.retention_days)
            
            cursor.execute('''
                DELETE FROM samples 
                WHERE sampled_at < ?
            ''', (cutoff_date.isoformat(),))
            
            deleted = cursor.rowcount
            conn.commit()
            conn.close()
            
            if deleted > 0:
                logger.info(f"Cleaned up {deleted} old records (older than {self.retention_days} days)")
            
        except sqlite3.OperationalError as e:
            logger.error(f"Cleanup error: {e}")
    
    def is_active_hours(self):
        """Check if within active hours"""
        now = datetime.now(zoneinfo.ZoneInfo("Asia/Singapore"))
        hour = now.hour
        return self.start_hour <= hour < self.end_hour
    
    def should_poll_location(self, location):
        """Check if location should be polled"""
        poll_interval = self.locations_config[location]['interval']
        last_poll = self.last_polls.get(location, datetime.min)
        time_elapsed = (datetime.now() - last_poll).total_seconds()
        return time_elapsed >= poll_interval
    
    def extract_and_queue(self):
        """Extract from API and queue for writing with staggered requests"""
        cleanup_counter = 0
        location_list = list(self.locations_config.keys())
        
        while self.running:
            if not self.is_active_hours():
                time.sleep(60)
                continue
            
            locations_to_poll = [
                loc for loc in location_list
                if self.should_poll_location(loc)
            ]
            
            if locations_to_poll:
                logger.info(f"Polling {len(locations_to_poll)} locations with {self.stagger_interval}s stagger")
            
            # Stagger requests to spread API load
            for location in locations_to_poll:
                try:
                    params = {self.location_param: location}
                    response = requests.get(
                        self.api_url, 
                        params=params, 
                        headers=self.headers,
                        timeout=15
                    )
                    response.raise_for_status()
                    
                    data = response.json()
                    self.records_processed += 1
                    self.last_polls[location] = datetime.now()
                    
                    priority = self.locations_config[location]['priority']
                    
                    record = {
                        'location': location,
                        'priority': priority,
                        'data': json.dumps(data),
                        'sampled_at': datetime.now().isoformat()
                    }
                    
                    try:
                        self.write_queue.put_nowait(record)
                        logger.info(f"Queued {location} ({priority}) - queue: {self.write_queue.qsize()}")
                    except:
                        logger.warning(f"Queue full, dropping {location}")
                    
                    # Stagger the next request to respect API rate limits
                    time.sleep(self.stagger_interval)
                    
                except requests.exceptions.HTTPError as e:
                    if e.response.status_code == 401:
                        logger.error(f"Authentication failed (401) for {location} - check API token")
                    elif e.response.status_code == 403:
                        logger.error(f"Permission denied (403) for {location} - check API token permissions")
                    elif e.response.status_code == 429:
                        logger.warning(f"Rate limited (429) for {location} - increase STAGGER_INTERVAL")
                    else:
                        logger.warning(f"HTTP error {e.response.status_code} for {location}: {e}")
                
                except requests.exceptions.RequestException as e:
                    logger.warning(f"Failed to poll {location}: {e}")
            
            cleanup_counter += 1
            if cleanup_counter >= 100:
                self.cleanup_old_data()
                cleanup_counter = 0
            
            time.sleep(1)
    
    def batch_write_to_db(self, batch_size=50):
        """Single writer thread that batches to SQLite"""
        while self.running:
            batch = []
            
            for _ in range(batch_size):
                try:
                    record = self.write_queue.get(timeout=5)
                    batch.append(record)
                except:
                    break
            
            if batch:
                self._write_batch(batch)
            else:
                time.sleep(1)
    
    def _write_batch(self, batch):
        """Write batch to database"""
        try:
            conn = sqlite3.connect(self.db_path, timeout=30)
            cursor = conn.cursor()
            
            cursor.executemany('''
                INSERT INTO samples (location, data, sampled_at, priority)
                VALUES (?, ?, ?, ?)
            ''', [
                (r['location'], r['data'], r['sampled_at'], r['priority'])
                for r in batch
            ])
            
            conn.commit()
            conn.close()
            
            logger.info(f"Wrote {len(batch)} records to database")
            
        except sqlite3.OperationalError as e:
            logger.error(f"Database write error: {e}")
            for record in batch:
                try:
                    self.write_queue.put_nowait(record)
                except:
                    pass
    
    def get_data(self, location=None, limit=100):
        """Query database"""
        try:
            conn = sqlite3.connect(self.db_path, timeout=30)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            if location:
                cursor.execute('''
                    SELECT * FROM samples 
                    WHERE location = ? 
                    ORDER BY sampled_at DESC 
                    LIMIT ?
                ''', (location, limit))
            else:
                cursor.execute('''
                    SELECT * FROM samples 
                    ORDER BY sampled_at DESC 
                    LIMIT ?
                ''', (limit,))
            
            rows = cursor.fetchall()
            conn.close()
            
            return [dict(row) for row in rows]
        
        except sqlite3.OperationalError as e:
            logger.error(f"Database read error: {e}")
            return []
    
    def get_stats(self):
        """Get statistics"""
        try:
            conn = sqlite3.connect(self.db_path, timeout=30)
            cursor = conn.cursor()
            
            cursor.execute('SELECT COUNT(*) FROM samples')
            total = cursor.fetchone()[0]
            
            cursor.execute('''
                SELECT location, COUNT(*) as count, MAX(sampled_at) as last_sample
                FROM samples 
                GROUP BY location
            ''')
            
            by_location = {row[0]: {'count': row[1], 'last_sample': row[2]} for row in cursor.fetchall()}
            
            cursor.execute('SELECT MAX(sampled_at) FROM samples')
            last_update = cursor.fetchone()[0]
            
            db_size_mb = os.path.getsize(self.db_path) / (1024 * 1024)
            
            conn.close()
            
            return {
                'total_records': total,
                'by_location': by_location,
                'last_update': last_update,
                'queue_size': self.write_queue.qsize(),
                'records_processed': self.records_processed,
                'db_size_mb': round(db_size_mb, 2),
                'retention_days': self.retention_days,
                'stagger_interval': self.stagger_interval
            }
        
        except sqlite3.OperationalError as e:
            logger.error(f"Database stats error: {e}")
            return {}
    
    def run(self):
        """Start both threads"""
        self.running = True
        
        logger.info("=== Starting Research ETL Pipeline ===")
        logger.info(f"API URL: {self.api_url}")
        logger.info(f"Location parameter: {self.location_param}")
        logger.info(f"Token header: {self.token_header}")
        logger.info(f"Database path: {self.db_path}")
        logger.info(f"Active hours: {self.start_hour}:00-{self.end_hour}:00")
        logger.info(f"Data retention: {self.retention_days} days")
        logger.info(f"Stagger interval: {self.stagger_interval}s (rate limiting)")
        logger.info("Location config:")
        
        total_daily_calls = 0
        for loc, config in self.locations_config.items():
            calls_per_day = (19 * 60 * 60) // config['interval']
            total_daily_calls += calls_per_day
        
        logger.info(f"Total locations: {len(self.locations_config)}")
        logger.info(f"Total estimated daily API calls: {total_daily_calls}")
        
        # Calculate expected requests per second
        rps = 1 / self.stagger_interval
        logger.info(f"Requests per second: {rps:.2f}")
        
        logger.info(f"=== Pipeline ready ===\n")
        
        writer_thread = threading.Thread(
            target=self.batch_write_to_db, 
            args=(50,),
            daemon=False
        )
        writer_thread.start()
        
        try:
            self.extract_and_queue()
        
        except KeyboardInterrupt:
            logger.info("Stopping...")
            self.running = False
            writer_thread.join(timeout=10)

def start_http_server(pipeline):
    """Expose data via HTTP"""
    from http.server import HTTPServer, BaseHTTPRequestHandler
    
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            path = self.path.split('?')[0]
            
            if path == '/data':
                location = None
                limit = 100
                
                if 'location=' in self.path:
                    location = self.path.split('location=')[1].split('&')[0]
                if 'limit=' in self.path:
                    try:
                        limit = int(self.path.split('limit=')[1].split('&')[0])
                    except:
                        pass
                
                data = pipeline.get_data(location=location, limit=limit)
                self.send_response(200)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                self.wfile.write(json.dumps(data).encode())
            
            elif path == '/stats':
                stats = pipeline.get_stats()
                self.send_response(200)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                self.wfile.write(json.dumps(stats, indent=2).encode())
            
            elif path == '/export':
                location = None
                if 'location=' in self.path:
                    location = self.path.split('location=')[1].split('&')[0]
                
                data = pipeline.get_data(location=location, limit=10000)
                self.send_response(200)
                self.send_header('Content-type', 'application/json')
                self.send_header('Content-Disposition', f'attachment; filename="research_{location or "all"}.json"')
                self.end_headers()
                self.wfile.write(json.dumps(data, indent=2).encode())
            
            else:
                self.send_response(404)
                self.end_headers()
        
        def log_message(self, format, *args):
            pass
    
    server = HTTPServer(('0.0.0.0', 8080), Handler)
    logger.info("HTTP server started on port 8080")
    server.serve_forever()

if __name__ == "__main__":
    import threading
    
    api_url = os.getenv('API_URL')
    api_token = os.getenv('API_TOKEN')
    token_header = os.getenv('API_TOKEN_HEADER', 'AccountKey')
    location_param = os.getenv('API_LOCATION_PARAM', 'BusStopCode')
    retention_days = int(os.getenv('DB_RETENTION_DAYS', '7'))
    db_path = os.getenv('DB_PATH', '/data/research.db')
    stagger_interval = float(os.getenv('STAGGER_INTERVAL', '0.5'))
    
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
        stagger_interval=stagger_interval
    )
    
    http_thread = threading.Thread(target=start_http_server, args=(pipeline,), daemon=True)
    http_thread.start()
    
    try:
        pipeline.run()
    finally:
        while True:  # Keeps the machine alive so you can check /stats
            time.sleep(3600)