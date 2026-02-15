#!/usr/bin/env python3
import os
from datetime import datetime
from pysqlcipher3 import dbapi2 as sqlite

ZNC_BASE_PATH = '/home/klapvogn/.znc/users/klapvogn/networks'
DB_PATH = '/home/klapvogn/apps/znc_search/znc_logs.db'
DB_KEY = '28ab2972b162ccc779d905cb6b422cd707d0470aef68c4289b41fa8ea42fb7df'
NETWORK_NAMES = {}

def get_db():
    conn = sqlite.connect(DB_PATH)
    conn.execute(f"PRAGMA key = '{DB_KEY}'")
    conn.execute("PRAGMA cipher_compatibility = 4")
    return conn

def parse_log_date(filename):
    date_str = filename.replace('.log', '')
    try:
        return datetime.strptime(date_str, '%Y-%m-%d')
    except ValueError:
        pass
    try:
        date_str = date_str.split('_')[-1]
        return datetime.strptime(date_str, '%Y%m%d')
    except ValueError:
        pass
    return None

def import_network(conn, network_id):
    cursor = conn.cursor()
    display_name = NETWORK_NAMES.get(network_id, network_id.capitalize())
    cursor.execute('INSERT OR REPLACE INTO networks (id, display_name) VALUES (?, ?)', (network_id, display_name))
    print(f"✓ Network: {network_id} -> {display_name}")
    
    log_base = os.path.join(ZNC_BASE_PATH, network_id, 'moddata/log')
    if not os.path.exists(log_base):
        print(f"⚠ No logs at: {log_base}")
        return 0
    
    total = 0
    for channel_name in os.listdir(log_base):
        channel_path = os.path.join(log_base, channel_name)
        if not os.path.isdir(channel_path):
            continue
        
        print(f"\n  Channel: {channel_name}")
        cursor.execute('INSERT OR IGNORE INTO channels (network_id, name) VALUES (?, ?)', (network_id, channel_name))
        
        for log_file in sorted([f for f in os.listdir(channel_path) if f.endswith('.log')]):
            log_date = parse_log_date(log_file)
            if not log_date:
                continue
            
            file_path = os.path.join(channel_path, log_file)
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                lines = f.readlines()
            
            entries = [(network_id, channel_name, log_date.strftime('%Y-%m-%d'), i+1, line.rstrip()) 
                      for i, line in enumerate(lines)]
            
            cursor.executemany('INSERT INTO log_entries (network_id, channel_name, log_date, line_number, content) VALUES (?, ?, ?, ?, ?)', entries)
            total += len(entries)
            print(f"    {log_file}: {len(entries)} lines")
    
    conn.commit()
    return total

conn = get_db()
count = import_network(conn, 'torrentleech')
print(f"\n✓ Total: {count:,} lines")
conn.close()