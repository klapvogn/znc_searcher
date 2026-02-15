#!/usr/bin/env python3
"""
Database utility script for ZNC log database management

This script provides common maintenance operations for the encrypted database.

Usage:
    python3 db_utils.py [command] [options]
    
Commands:
    stats       - Show database statistics
    vacuum      - Optimize database file
    reindex     - Rebuild all indexes
    verify      - Verify database integrity
    export      - Export database to plaintext SQL
    backup      - Create encrypted backup (saved to backup/ directory)
    cleanup     - Remove old backups (default: older than 30 days)
"""

import os
import sys
import argparse
from datetime import datetime
from pysqlcipher3 import dbapi2 as sqlite
import shutil

# Configuration - should match app.py
DB_PATH = '/home/klapvogn/apps/znc_search/znc_logs.db'
DB_KEY = '28ab2972b162ccc779d905cb6b422cd707d0470aef68c4289b41fa8ea42fb7df'  # Must match app.py
BACKUP_DIR = 'backup'  # Directory for backups (relative to script location)

def get_db():
    """Get database connection with encryption"""
    if not os.path.exists(DB_PATH):
        print(f"Error: Database not found: {DB_PATH}")
        sys.exit(1)
    
    conn = sqlite.connect(DB_PATH)
    conn.execute(f"PRAGMA key = '{DB_KEY}'")
    conn.execute("PRAGMA cipher_compatibility = 4")
    return conn

def show_stats():
    """Show detailed database statistics"""
    conn = get_db()
    cursor = conn.cursor()
    
    print("\n" + "=" * 70)
    print("DATABASE STATISTICS")
    print("=" * 70)
    
    # Total entries
    cursor.execute('SELECT COUNT(*) FROM log_entries')
    total_entries = cursor.fetchone()[0]
    print(f"Total log entries: {total_entries:,}")
    
    # Networks
    cursor.execute('SELECT COUNT(*) FROM networks')
    network_count = cursor.fetchone()[0]
    print(f"Networks: {network_count}")
    
    # Channels
    cursor.execute('SELECT COUNT(DISTINCT channel_name) FROM log_entries')
    channel_count = cursor.fetchone()[0]
    print(f"Unique channels: {channel_count}")
    
    # Date range
    cursor.execute('SELECT MIN(log_date), MAX(log_date) FROM log_entries')
    date_range = cursor.fetchone()
    print(f"Date range: {date_range[0]} to {date_range[1]}")
    
    # Last import
    cursor.execute('SELECT value FROM import_metadata WHERE key = ?', ('last_import_date',))
    row = cursor.fetchone()
    if row:
        print(f"Last import: {row[0]}")
    
    # Database file size
    db_size = os.path.getsize(DB_PATH)
    print(f"Database file size: {db_size / (1024*1024):.2f} MB")
    
    print("\n" + "-" * 70)
    print("ENTRIES PER NETWORK")
    print("-" * 70)
    
    cursor.execute('''
        SELECT n.display_name, COUNT(*) as count
        FROM log_entries le
        JOIN networks n ON le.network_id = n.id
        GROUP BY n.display_name
        ORDER BY count DESC
    ''')
    
    for row in cursor.fetchall():
        print(f"  {row[0]}: {row[1]:,} entries")
    
    print("\n" + "-" * 70)
    print("TOP 10 CHANNELS BY ACTIVITY")
    print("-" * 70)
    
    cursor.execute('''
        SELECT channel_name, COUNT(*) as count
        FROM log_entries
        GROUP BY channel_name
        ORDER BY count DESC
        LIMIT 10
    ''')
    
    for row in cursor.fetchall():
        print(f"  {row[0]}: {row[1]:,} entries")
    
    conn.close()
    print()

def vacuum_db():
    """Optimize database file"""
    print("\nVacuuming database...")
    conn = get_db()
    
    before_size = os.path.getsize(DB_PATH)
    
    conn.execute('VACUUM')
    conn.close()
    
    after_size = os.path.getsize(DB_PATH)
    saved = before_size - after_size
    
    print(f"✓ Vacuum complete")
    print(f"  Before: {before_size / (1024*1024):.2f} MB")
    print(f"  After:  {after_size / (1024*1024):.2f} MB")
    print(f"  Saved:  {saved / (1024*1024):.2f} MB")

def reindex_db():
    """Rebuild all indexes"""
    print("\nRebuilding indexes...")
    conn = get_db()
    
    conn.execute('REINDEX')
    conn.close()
    
    print("✓ Reindex complete")

def verify_db():
    """Verify database integrity"""
    print("\nVerifying database integrity...")
    conn = get_db()
    cursor = conn.cursor()
    
    # Integrity check
    cursor.execute('PRAGMA integrity_check')
    result = cursor.fetchone()[0]
    
    if result == 'ok':
        print("✓ Database integrity check: PASSED")
    else:
        print(f"✗ Database integrity check: FAILED - {result}")
        conn.close()
        return False
    
    # Foreign key check
    cursor.execute('PRAGMA foreign_key_check')
    violations = cursor.fetchall()
    
    if not violations:
        print("✓ Foreign key check: PASSED")
    else:
        print(f"✗ Foreign key check: FAILED - {len(violations)} violations found")
        for v in violations[:5]:  # Show first 5
            print(f"  {v}")
    
    conn.close()
    return True

def backup_db(output_path=None):
    """Create encrypted backup of database"""
    # Create backup directory if it doesn't exist
    os.makedirs(BACKUP_DIR, exist_ok=True)
    
    if output_path is None:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_path = os.path.join(BACKUP_DIR, f'znc_logs_backup_{timestamp}.db')
    
    print(f"\nCreating backup: {output_path}")
    
    try:
        shutil.copy2(DB_PATH, output_path)
        
        backup_size = os.path.getsize(output_path)
        print(f"✓ Backup created successfully")
        print(f"  Size: {backup_size / (1024*1024):.2f} MB")
        print(f"  Path: {os.path.abspath(output_path)}")
        
    except Exception as e:
        print(f"✗ Backup failed: {e}")
        return False
    
    return True

def export_db(output_path=None):
    """Export database to SQL file (WARNING: Not encrypted!)"""
    if output_path is None:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_path = f'znc_logs_export_{timestamp}.sql'
    
    print(f"\nExporting database to: {output_path}")
    print("WARNING: The exported SQL file will NOT be encrypted!")
    
    try:
        conn = get_db()
        
        with open(output_path, 'w', encoding='utf-8') as f:
            for line in conn.iterdump():
                f.write(f"{line}\n")
        
        conn.close()
        
        export_size = os.path.getsize(output_path)
        print(f"✓ Export completed successfully")
        print(f"  Size: {export_size / (1024*1024):.2f} MB")
        print(f"  Path: {os.path.abspath(output_path)}")
        
    except Exception as e:
        print(f"✗ Export failed: {e}")
        return False
    
    return True

def cleanup_backups(keep_days=30):
    """Remove backups older than specified days"""
    if not os.path.exists(BACKUP_DIR):
        print(f"\nNo backup directory found at: {BACKUP_DIR}")
        return
    
    print(f"\nCleaning up backups older than {keep_days} days...")
    
    cutoff_time = datetime.now().timestamp() - (keep_days * 86400)
    removed_count = 0
    removed_size = 0
    
    for filename in os.listdir(BACKUP_DIR):
        if not filename.startswith('znc_logs_backup_'):
            continue
        
        filepath = os.path.join(BACKUP_DIR, filename)
        file_mtime = os.path.getmtime(filepath)
        
        if file_mtime < cutoff_time:
            file_size = os.path.getsize(filepath)
            os.remove(filepath)
            removed_count += 1
            removed_size += file_size
            print(f"  Removed: {filename}")
    
    if removed_count > 0:
        print(f"\n✓ Cleanup complete")
        print(f"  Removed {removed_count} backup(s)")
        print(f"  Freed {removed_size / (1024*1024):.2f} MB")
    else:
        print("  No old backups to remove")

def main():
    parser = argparse.ArgumentParser(description='ZNC Log Database Utilities')
    parser.add_argument('command', 
                       choices=['stats', 'vacuum', 'reindex', 'verify', 'export', 'backup', 'cleanup'],
                       help='Command to execute')
    parser.add_argument('-o', '--output', 
                       help='Output file path (for export/backup)')
    parser.add_argument('--keep-days', type=int, default=30,
                       help='Days to keep backups (for cleanup, default: 30)')
    
    args = parser.parse_args()
    
    if args.command == 'stats':
        show_stats()
    elif args.command == 'vacuum':
        vacuum_db()
    elif args.command == 'reindex':
        reindex_db()
    elif args.command == 'verify':
        verify_db()
    elif args.command == 'backup':
        backup_db(args.output)
    elif args.command == 'export':
        export_db(args.output)
    elif args.command == 'cleanup':
        cleanup_backups(args.keep_days)

if __name__ == '__main__':
    main()