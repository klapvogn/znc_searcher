#!/usr/bin/env python3
"""
Import ZNC logs into MySQL database
"""

import os
import sys
import re
from datetime import datetime
import mysql.connector
from mysql.connector import pooling
import argparse
from dotenv import load_dotenv
import getpass

load_dotenv()

# Configuration
USERNAME = os.getenv('USERNAME', getpass.getuser())
ZNC_BASE_PATH = os.path.expanduser(os.getenv('ZNC_BASE_PATH', f'~/.znc/users/{USERNAME}/networks'))

MYSQL_HOST = os.getenv('MYSQL_HOST', 'localhost')
MYSQL_PORT = int(os.getenv('MYSQL_PORT', 3306))
MYSQL_USER = os.getenv('MYSQL_USER', 'root')
MYSQL_PASSWORD = os.getenv('MYSQL_PASSWORD', '')
MYSQL_DATABASE = os.getenv('MYSQL_DATABASE', 'znc_logs')

# Batch size for bulk inserts
BATCH_SIZE = 5000


SKIP_PREFIXES = (
    '*** Joins:',
    '*** Parts:',
    '*** Quits:',
    '*** Now talking in',
    '*** Topic is:',
    '*** Set by',
    '*** Joins:',
    '*** ChanServ sets',
)

SKIP_NICKS = {
    'chanserv', 'nickserv', 'hostserv', 'memoserv', 'operserv', 'global'
}

def should_skip(line):
    """Return True if line should not be stored"""
    # Strip timestamp if present e.g. [08:25:58] 
    stripped = re.sub(r'^\[\d{2}:\d{2}:\d{2}\]\s*', '', line)

    # Skip server/status messages
    for prefix in SKIP_PREFIXES:
        if stripped.startswith(prefix):
            return True

    # Skip mode changes and kicks
    if stripped.startswith('*** '):
        return True

    # Skip service bots by nick - matches "<NickServ> ..." format
    nick_match = re.match(r'^[<*]\s*([A-Za-z0-9_\-\[\]\\`^{}|]+)[>*]?\s', stripped)
    if nick_match:
        nick = nick_match.group(1).lower()
        if nick in SKIP_NICKS:
            return True

    return False


def strip_irc_formatting(text):
    """Remove IRC color codes and formatting characters"""
    text = re.sub(r'\x03(?:\d{1,2}(?:,\d{1,2})?)?', '', text)
    text = re.sub(r'[\x02\x1D\x1F\x16\x0F]', '', text)
    return text


def get_db():
    """Get MySQL database connection"""
    return mysql.connector.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DATABASE,
        charset='utf8mb4',
        collation='utf8mb4_unicode_ci'
    )


def init_db():
    """Initialize default admin user if not exists"""
    import hashlib
    conn = get_db()
    cursor = conn.cursor()

    cursor.execute('SELECT COUNT(*) FROM users WHERE username = %s', ('admin',))
    if cursor.fetchone()[0] == 0:
        default_hash = hashlib.sha256('admin'.encode()).hexdigest()
        cursor.execute(
            'INSERT INTO users (username, password_hash, totp_enabled) VALUES (%s, %s, 0)',
            ('admin', default_hash)
        )
        conn.commit()
        print("WARNING: Default admin user created with password 'admin'. Please change it immediately!")

    cursor.close()
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

    display_name = network_id.capitalize()

    cursor.execute(
        'INSERT INTO networks (id, display_name) VALUES (%s, %s) '
        'ON DUPLICATE KEY UPDATE display_name = VALUES(display_name)',
        (network_id, display_name)
    )

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

        cursor.execute(
            'INSERT IGNORE INTO channels (network_id, name) VALUES (%s, %s)',
            (network_id, channel_name)
        )

        log_files = sorted([f for f in os.listdir(channel_path) if f.endswith('.log')])

        for log_file in log_files:
            log_date = parse_log_date(log_file)

            if not log_date:
                continue

            file_path = os.path.join(channel_path, log_file)
            date_str = log_date.strftime('%Y-%m-%d')

            # Check if already imported
            if not force:
                cursor.execute(
                    'SELECT COUNT(*) FROM log_entries '
                    'WHERE network_id = %s AND channel_name = %s AND log_date = %s',
                    (network_id, channel_name, date_str)
                )
                if cursor.fetchone()[0] > 0:
                    print(f"    ⊘ {log_file}: already imported")
                    continue

            try:
                with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                    lines = f.readlines()

                if not lines:
                    continue

                entries = [
                    (network_id, channel_name, date_str, i + 1, strip_irc_formatting(line.rstrip()))
                    for i, line in enumerate(lines)
                    if not should_skip(line)
                ]

                # Insert in batches
                rows_inserted = 0
                for i in range(0, len(entries), BATCH_SIZE):
                    batch = entries[i:i + BATCH_SIZE]
                    cursor.executemany(
                        'INSERT INTO log_entries '
                        '(network_id, channel_name, log_date, line_number, content) '
                        'VALUES (%s, %s, %s, %s, %s)',
                        batch
                    )
                    rows_inserted += len(batch)

                conn.commit()
                total_imported += rows_inserted
                print(f"    ✓ {log_file}: {rows_inserted} lines")

            except Exception as e:
                print(f"    ✗ Error reading {log_file}: {e}")
                conn.rollback()
                continue

    return total_imported


def main():
    parser = argparse.ArgumentParser(description='Import ZNC logs to MySQL database')
    parser.add_argument('--network', type=str, help='Import only specific network')
    parser.add_argument('--force', action='store_true', help='Force re-import of existing logs')
    args = parser.parse_args()

    if not os.path.exists(ZNC_BASE_PATH):
        print(f"Error: ZNC base path not found: {ZNC_BASE_PATH}")
        sys.exit(1)

    try:
        conn = get_db()
    except mysql.connector.Error as e:
        print(f"Error connecting to MySQL: {e}")
        sys.exit(1)

    init_db()

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

    # Stats
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