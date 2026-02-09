#!/usr/bin/env python3
"""
Test CSV/JSON configuration loading
Usage: python test_config.py
"""

import csv
import json
import os
import sys

def test_csv_config(filename='locations.csv'):
    """Test loading CSV config"""
    print(f"\n📋 Testing CSV Configuration: {filename}")
    print(f"{'='*50}\n")
    
    if not os.path.exists(filename):
        print(f"❌ File not found: {filename}")
        return False
    
    try:
        locations_config = {}
        with open(filename, 'r') as f:
            reader = csv.DictReader(f)
            
            # Check headers
            if reader.fieldnames != ['location', 'interval', 'priority']:
                print(f"❌ Invalid CSV headers: {reader.fieldnames}")
                print(f"   Expected: ['location', 'interval', 'priority']")
                return False
            
            for row in reader:
                try:
                    location = row['location'].strip()
                    interval = int(row['interval'])
                    priority = row['priority'].strip().lower()
                    
                    if priority not in ['high', 'medium', 'low']:
                        print(f"❌ Invalid priority '{priority}' for {location}")
                        return False
                    
                    if interval <= 0:
                        print(f"❌ Invalid interval {interval} for {location}")
                        return False
                    
                    locations_config[location] = {
                        'interval': interval,
                        'priority': priority
                    }
                
                except ValueError as e:
                    print(f"❌ Invalid data in row: {row}")
                    print(f"   {e}")
                    return False
        
        if not locations_config:
            print(f"❌ No locations found in {filename}")
            return False
        
        print(f"✓ Loaded {len(locations_config)} locations:\n")
        
        total_daily_calls = 0
        for loc, config in sorted(locations_config.items()):
            calls_per_day = (19 * 60 * 60) // config['interval']
            total_daily_calls += calls_per_day
            print(f"  ✓ {loc:20} interval={config['interval']:5}s  priority={config['priority']:6}  ~{calls_per_day:5} calls/day")
        
        print(f"\n  Total estimated daily calls: {total_daily_calls:,}")
        print(f"  Daily API quota used: {total_daily_calls/10_000_000*100:.1f}% of 10M\n")
        
        if total_daily_calls > 10_000_000:
            print(f"⚠ WARNING: Exceeds 10M daily limit!")
            print(f"   Reduce intervals or remove locations")
            return False
        
        print("✅ CSV config test PASSED")
        return True
    
    except Exception as e:
        print(f"❌ Config test FAILED: {e}")
        return False

def test_json_config(filename='locations.json'):
    """Test loading JSON config"""
    print(f"\n📋 Testing JSON Configuration: {filename}")
    print(f"{'='*50}\n")
    
    if not os.path.exists(filename):
        print(f"⚠ File not found: {filename} (optional)")
        return True
    
    try:
        with open(filename, 'r') as f:
            locations_config = json.load(f)
        
        if not locations_config:
            print(f"❌ No locations found in {filename}")
            return False
        
        print(f"✓ Loaded {len(locations_config)} locations:\n")
        
        total_daily_calls = 0
        for loc, config in sorted(locations_config.items()):
            if 'interval' not in config or 'priority' not in config:
                print(f"❌ Missing 'interval' or 'priority' for {loc}")
                return False
            
            interval = config['interval']
            priority = config['priority'].lower()
            
            if priority not in ['high', 'medium', 'low']:
                print(f"❌ Invalid priority '{priority}' for {loc}")
                return False
            
            if not isinstance(interval, int) or interval <= 0:
                print(f"❌ Invalid interval {interval} for {loc}")
                return False
            
            calls_per_day = (19 * 60 * 60) // interval
            total_daily_calls += calls_per_day
            print(f"  ✓ {loc:20} interval={interval:5}s  priority={priority:6}  ~{calls_per_day:5} calls/day")
        
        print(f"\n  Total estimated daily calls: {total_daily_calls:,}")
        print(f"  Daily API quota used: {total_daily_calls/10_000_000*100:.1f}% of 10M\n")
        
        if total_daily_calls > 10_000_000:
            print(f"⚠ WARNING: Exceeds 10M daily limit!")
            return False
        
        print("✅ JSON config test PASSED")
        return True
    
    except json.JSONDecodeError as e:
        print(f"❌ Invalid JSON: {e}")
        return False
    except Exception as e:
        print(f"❌ Config test FAILED: {e}")
        return False

if __name__ == "__main__":
    csv_ok = test_csv_config()
    json_ok = test_json_config()
    
    print(f"\n{'='*50}")
    if csv_ok:
        print("✅ Configuration tests PASSED")
        sys.exit(0)
    else:
        print("❌ Configuration tests FAILED")
        sys.exit(1)
