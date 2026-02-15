#!/usr/bin/env python3
"""
Diagnostic script to check ZNC log structure
"""

import os
import getpass
from datetime import datetime

USERNAME = os.getenv('USERNAME', getpass.getuser())
ZNC_BASE_PATH = os.path.expanduser(os.getenv('ZNC_BASE_PATH', f'~/.znc/users/{USERNAME}/networks'))

def parse_log_date(filename):
    """Parse date from log filename"""
    date_str = filename.replace('.log', '')
    
    # Try format with dashes first (2025-12-04)
    try:
        result = datetime.strptime(date_str, '%Y-%m-%d')
        print(f"  ✓ Parsed {filename} as {result} (format: YYYY-MM-DD)")
        return result
    except ValueError:
        pass
    
    # Try format without dashes (20251204 or channel_20251204)
    try:
        date_str_split = date_str.split('_')[-1]
        result = datetime.strptime(date_str_split, '%Y%m%d')
        print(f"  ✓ Parsed {filename} as {result} (format: YYYYMMDD)")
        return result
    except ValueError:
        pass
    
    print(f"  ✗ FAILED to parse: {filename}")
    return None

print("Checking ZNC log structure...")
print("=" * 70)

for network_id in os.listdir(ZNC_BASE_PATH):
    network_path = os.path.join(ZNC_BASE_PATH, network_id)
    if not os.path.isdir(network_path):
        continue
    
    print(f"\nNetwork: {network_id}")
    
    log_base = os.path.join(network_path, 'moddata/log')
    
    if not os.path.exists(log_base):
        print(f"  ✗ No log directory found at: {log_base}")
        continue
    
    print(f"  Log directory: {log_base}")
    
    # Check channels
    for channel_name in os.listdir(log_base):
        channel_path = os.path.join(log_base, channel_name)
        
        if not os.path.isdir(channel_path):
            continue
        
        print(f"\n  Channel: {channel_name}")
        
        # List first few log files
        log_files = sorted([f for f in os.listdir(channel_path) if f.endswith('.log')])
        
        if not log_files:
            print(f"    ✗ No .log files found")
            continue
        
        print(f"    Found {len(log_files)} log files")
        print(f"    Checking first few files:")
        
        for log_file in log_files[:3]:
            file_path = os.path.join(channel_path, log_file)
            
            # Check file size
            size = os.path.getsize(file_path)
            print(f"\n    File: {log_file} ({size} bytes)")
            
            # Try to parse date
            log_date = parse_log_date(log_file)
            
            # Show first few lines
            if size > 0:
                try:
                    with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                        lines = [f.readline().strip() for _ in range(3)]
                    print(f"    First lines:")
                    for line in lines:
                        if line:
                            print(f"      {line[:80]}")
                except Exception as e:
                    print(f"    ✗ Error reading file: {e}")
            else:
                print(f"    ⚠ File is empty!")


print("\n" + "=" * 70)
