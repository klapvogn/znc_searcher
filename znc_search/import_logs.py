#!/usr/bin/env python3
"""
Import ZNC logs into encrypted SQLite database
"""

import os
import sys
import re
from datetime import datetime
from pysqlcipher3 import dbapi2 as sqlite
import argparse
from dotenv import load_dotenv
import getpass

# Load environment variables from .env file
load_dotenv()

# Configuration
USERNAME = os.getenv('USERNAME', getpass.getuser())
ZNC_BASE_PATH = os.path.expanduser(os.getenv('ZNC_BASE_PATH', f'~/.znc/users/{USERNAME}/networks'))
DB_PATH = os.path.expanduser(os.getenv('DB_PATH', '~/apps/znc_search/znc_logs.db'))
DB_KEY = os.getenv('DB_KEY')

# Network display name mapping
NETWORK_NAMES = {
    'torrentleech': 'Torrentleech',
    'mam': 'MAM'
}

def strip_irc_formatting(text):
    """Remove IRC color codes and formatting characters"""
    # Remove color codes: \x03 followed by optional foreground[,background] digits
    text = re.sub(r'\x03(?:\d{1,2}(?:,\d{1,2})?)?', '', text)
    
    # Remove other formatting codes:
    # \x02 = bold
    # \x1D = italic
    # \x1F = underline
    # \x16 = reverse
    # \x0F = reset/normal
    text = re.sub(r'[\x02\x1D\x1F\x16\x0F]', '', text)
    
    return text

def get_db():
    """Get database connection with encryption"""
    conn = sqlite.connect(DB_PATH)
    conn.execute(f"PRAGMA key = '{DB_KEY}'")
    conn.execute("PRAGMA cipher_compatibility = 4")
    return conn

def init_db():
    """Initialize the database schema"""
    conn = get_db()
    cursor = conn.cursor()
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS networks (
            id TEXT PRIMARY KEY,
            display_name TEXT NOT NULL
        )
    ''')
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS channels (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            network_id TEXT NOT NULL,
            name TEXT NOT NULL,
            FOREIGN KEY (network_id) REFERENCES networks(id),
            UNIQUE(network_id, name)
        )
    ''')
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS log_entries (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            network_id TEXT NOT NULL,
            channel_name TEXT NOT NULL,
            log_date DATE NOT NULL,
            line_number INTEGER NOT NULL,
            content TEXT NOT NULL,
            FOREIGN KEY (network_id) REFERENCES networks(id)
        )
    ''')

    # Create users table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL,
            totp_secret TEXT,
            totp_enabled INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    # Check if default admin user exists, if not create it
    cursor.execute('SELECT COUNT(*) FROM users WHERE username = ?', ('admin',))
    if cursor.fetchone()[0] == 0:
        # Default password is 'admin' - CHANGE THIS IMMEDIATELY!
        import hashlib
        default_password_hash = hashlib.sha256('admin'.encode()).hexdigest()
        cursor.execute('''
            INSERT INTO users (username, password_hash, totp_enabled) 
            VALUES (?, ?, 0)
        ''', ('admin', default_password_hash))
        print("WARNING: Default admin user created with password 'admin'. Please change it immediately!")    
    
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_log_network ON log_entries(network_id)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_log_channel ON log_entries(channel_name)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_log_date ON log_entries(log_date)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_log_content ON log_entries(content)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_log_composite ON log_entries(network_id, channel_name, log_date)')
    
    conn.commit()
    conn.close()

def parse_log_date(filename):
    """Parse date from log filename"""
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

def import_network(conn, network_id, force=False):
    """Import logs for a single network"""
    cursor = conn.cursor()
    
    display_name = NETWORK_NAMES.get(network_id, network_id.capitalize())
    cursor.execute('INSERT OR REPLACE INTO networks (id, display_name) VALUES (?, ?)', 
                   (network_id, display_name))
    
    log_base = os.path.join(ZNC_BASE_PATH, network_id, 'moddata/log')
    
    if not os.path.exists(log_base):
        print(f"  ⚠ Log directory not found: {log_base}")
        return 0
    
    print(f"Importing network: {network_id} ({display_name})")
    total_imported = 0
    
    for channel_name in sorted(os.listdir(log_base)):
        channel_path = os.path.join(log_base, channel_name)
        
        if not os.path.isdir(channel_path):
            continue
        
        print(f"  Processing channel: {channel_name}")
        
        cursor.execute('INSERT OR IGNORE INTO channels (network_id, name) VALUES (?, ?)', 
                      (network_id, channel_name))
        
        log_files = sorted([f for f in os.listdir(channel_path) if f.endswith('.log')])
        
        for log_file in log_files:
            log_date = parse_log_date(log_file)
            
            if not log_date:
                continue
            
            file_path = os.path.join(channel_path, log_file)
            date_str = log_date.strftime('%Y-%m-%d')
            
            # Check if already imported
            if not force:
                cursor.execute('''
                    SELECT COUNT(*) FROM log_entries 
                    WHERE network_id = ? AND channel_name = ? AND log_date = ?
                ''', (network_id, channel_name, date_str))
                
                if cursor.fetchone()[0] > 0:
                    print(f"    ⊘ {log_file}: already imported")
                    continue
            
            # Read and import file
            try:
                with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                    lines = f.readlines()
                
                if not lines:
                    continue
                
                # Strip IRC formatting from each line
                entries = [
                    (network_id, channel_name, date_str, i+1, strip_irc_formatting(line.rstrip()))
                    for i, line in enumerate(lines)
                ]
                
                cursor.executemany('''
                    INSERT INTO log_entries 
                    (network_id, channel_name, log_date, line_number, content)
                    VALUES (?, ?, ?, ?, ?)
                ''', entries)

                rows_inserted = cursor.rowcount
                total_imported += rows_inserted
                print(f"    ✓ {log_file}: {rows_inserted} lines")

            except Exception as e:
                print(f"    ✗ Error reading {log_file}: {e}")
                continue
        
        # Commit after each channel
        conn.commit()
    
    return total_imported

def main():
    parser = argparse.ArgumentParser(description='Import ZNC logs to encrypted SQLite database')
    parser.add_argument('--network', type=str, help='Import only specific network')
    parser.add_argument('--force', action='store_true', help='Force re-import of existing logs')
    args = parser.parse_args()
    
    if not os.path.exists(ZNC_BASE_PATH):
        print(f"Error: ZNC base path not found: {ZNC_BASE_PATH}")
        sys.exit(1)
    
    # Initialize database if needed
    if not os.path.exists(DB_PATH):
        print("Initializing database...")
        init_db()
    
    conn = get_db()
    
    # Get networks to import
    if args.network:
        networks = [args.network]
    else:
        networks = [d for d in os.listdir(ZNC_BASE_PATH) 
                   if os.path.isdir(os.path.join(ZNC_BASE_PATH, d))]
    
    print(f"Scanning ZNC logs from: {ZNC_BASE_PATH}")
    print("=" * 70)
    
    total_imported = 0
    for network_id in sorted(networks):
        count = import_network(conn, network_id, args.force)
        total_imported += count
        print(f"  Network total: {count:,} lines\n")
    
    # Get statistics
    cursor = conn.cursor()
    cursor.execute('SELECT COUNT(*) FROM log_entries')
    total_entries = cursor.fetchone()[0]
    
    cursor.execute('SELECT COUNT(DISTINCT network_id) FROM log_entries')
    network_count = cursor.fetchone()[0]
    
    cursor.execute('SELECT COUNT(DISTINCT channel_name) FROM log_entries')
    channel_count = cursor.fetchone()[0]
    
    cursor.execute('SELECT MIN(log_date), MAX(log_date) FROM log_entries')
    date_range = cursor.fetchone()
    
    conn.close()
    
    print("=" * 70)
    print("Import complete!")
    print(f"  New lines imported: {total_imported:,}")
    print(f"  Total lines in database: {total_entries:,}")
    print(f"  Networks: {network_count}")
    print(f"  Channels: {channel_count}")
    if date_range[0]:
        print(f"  Date range: {date_range[0]} to {date_range[1]}")

if __name__ == '__main__':
    main()