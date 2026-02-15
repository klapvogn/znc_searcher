#!/usr/bin/env python3
"""
Migration script to add users table to existing IRC log database

This script will:
1. Create the users table if it doesn't exist
2. Create a default admin user with password 'admin'
3. Not affect any existing log data

Usage:
    python3 migrate_add_users.py
"""

import os
import sys
import hashlib
from pysqlcipher3 import dbapi2 as sqlite

# Configuration - should match app.py
DB_PATH = '/home/klapvogn/apps/znc_search/znc_logs.db'
DB_KEY = '28ab2972b162ccc779d905cb6b422cd707d0470aef68c4289b41fa8ea42fb7df'

def hash_password(password):
    """Hash password using SHA256"""
    return hashlib.sha256(password.encode()).hexdigest()

def get_db():
    """Get database connection with encryption"""
    if not os.path.exists(DB_PATH):
        print(f"Error: Database not found: {DB_PATH}")
        print(f"Expected location: {DB_PATH}")
        sys.exit(1)
    
    conn = sqlite.connect(DB_PATH)
    conn.execute(f"PRAGMA key = '{DB_KEY}'")
    conn.execute("PRAGMA cipher_compatibility = 4")
    return conn

def migrate():
    """Add users table and default admin user"""
    print("\n" + "=" * 70)
    print("IRC LOG SEARCH - USER TABLE MIGRATION")
    print("=" * 70)
    print()
    
    try:
        conn = get_db()
        cursor = conn.cursor()
        
        # Check if users table already exists
        cursor.execute('''
            SELECT name FROM sqlite_master 
            WHERE type='table' AND name='users'
        ''')
        
        if cursor.fetchone():
            print("⚠ Users table already exists. Checking for admin user...")
            
            cursor.execute('SELECT COUNT(*) FROM users WHERE username = ?', ('admin',))
            if cursor.fetchone()[0] > 0:
                print("✓ Admin user already exists.")
                print("\nNo migration needed. Everything is already set up!")
                conn.close()
                return
            else:
                print("ℹ Users table exists but no admin user found. Creating admin user...")
        else:
            print("Creating users table...")
            
            # Create users table
            cursor.execute('''
                CREATE TABLE users (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    username TEXT UNIQUE NOT NULL,
                    password_hash TEXT NOT NULL,
                    totp_secret TEXT,
                    totp_enabled INTEGER DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            print("✓ Users table created successfully")
        
        # Create default admin user
        print("\nCreating default admin user...")
        default_password_hash = hash_password('admin')
        
        cursor.execute('''
            INSERT INTO users (username, password_hash, totp_enabled) 
            VALUES (?, ?, 0)
        ''', ('admin', default_password_hash))
        
        conn.commit()
        conn.close()
        
        print("✓ Default admin user created")
        print()
        print("=" * 70)
        print("MIGRATION COMPLETE!")
        print("=" * 70)
        print()
        print("Default login credentials:")
        print("  Username: admin")
        print("  Password: admin")
        print()
        print("⚠ WARNING: Please change the default password immediately after login!")
        print("           Go to Settings → Change Password")
        print()
        
    except sqlite.Error as e:
        print(f"\n✗ Database error: {e}")
        print("\nPossible issues:")
        print("  - Incorrect database path")
        print("  - Incorrect encryption key")
        print("  - Database is corrupted")
        sys.exit(1)
    except Exception as e:
        print(f"\n✗ Unexpected error: {e}")
        sys.exit(1)

if __name__ == '__main__':
    migrate()