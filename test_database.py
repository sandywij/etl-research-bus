#!/usr/bin/env python3
"""
Test database creation and basic operations
Usage: python test_database.py
"""

import sqlite3
import os
import sys
import json
from datetime import datetime

def test_database(db_path='test_research.db'):
    """Test database creation and operations"""
    print(f"\n📋 Testing Database: {db_path}")
    print(f"{'='*50}\n")
    
    # Clean up old test DB
    if os.path.exists(db_path):
        os.remove(db_path)
    
    try:
        # Create connection
        print("⏳ Creating database...")
        conn = sqlite3.connect(db_path, timeout=30)
        cursor = conn.cursor()
        
        # Enable WAL
        print("✓ Enabling WAL mode...")
        cursor.execute('PRAGMA journal_mode=WAL')
        result = cursor.fetchone()[0]
        if result != 'wal':
            print(f"❌ WAL mode failed: {result}")
            return False
        
        # Create table
        print("✓ Creating samples table...")
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
        
        # Create index
        print("✓ Creating index...")
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_location_sampled 
            ON samples(location, sampled_at)
        ''')
        
        conn.commit()
        
        # Test insert
        print("⏳ Testing insert...")
        test_data = {
            'temperature': 25.5,
            'humidity': 65.3,
            'location': 'test'
        }
        
        cursor.execute('''
            INSERT INTO samples (location, data, sampled_at, priority)
            VALUES (?, ?, ?, ?)
        ''', (
            'TestLocation',
            json.dumps(test_data),
            datetime.now().isoformat(),
            'high'
        ))
        
        conn.commit()
        print("✓ Insert successful")
        
        # Test read
        print("⏳ Testing read...")
        cursor.execute('SELECT COUNT(*) FROM samples')
        count = cursor.fetchone()[0]
        
        if count != 1:
            print(f"❌ Record count mismatch: expected 1, got {count}")
            return False
        
        print("✓ Read successful")
        
        # Test query
        print("⏳ Testing query...")
        cursor.execute('SELECT * FROM samples WHERE location = ?', ('TestLocation',))
        row = cursor.fetchone()
        
        if not row:
            print(f"❌ Query returned no results")
            return False
        
        print("✓ Query successful")
        
        # Test update
        print("⏳ Testing update...")
        cursor.execute('UPDATE samples SET priority = ? WHERE id = ?', ('low', row[0]))
        conn.commit()
        print("✓ Update successful")
        
        # Test delete
        print("⏳ Testing delete...")
        cursor.execute('DELETE FROM samples WHERE id = ?', (row[0],))
        conn.commit()
        
        cursor.execute('SELECT COUNT(*) FROM samples')
        count = cursor.fetchone()[0]
        
        if count != 0:
            print(f"❌ Delete failed: expected 0 records, got {count}")
            return False
        
        print("✓ Delete successful")
        
        conn.close()
        
        # Check file size
        size_mb = os.path.getsize(db_path) / (1024 * 1024)
        print(f"\n✓ Database file created: {db_path}")
        print(f"  Size: {size_mb:.3f} MB")
        
        # Cleanup
        os.remove(db_path)
        print(f"✓ Test database cleaned up")
        
        print(f"\n✅ Database test PASSED")
        return True
    
    except sqlite3.Error as e:
        print(f"❌ Database error: {e}")
        return False
    
    except Exception as e:
        print(f"❌ Database test FAILED: {e}")
        return False
    
    finally:
        try:
            conn.close()
        except:
            pass

if __name__ == "__main__":
    success = test_database()
    sys.exit(0 if success else 1)
