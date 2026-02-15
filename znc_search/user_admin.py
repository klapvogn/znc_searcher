#!/usr/bin/env python3
"""
User Management Utility for IRC Log Search

This script provides command-line tools for managing users in the encrypted database.

Usage:
    python3 user_admin.py [command] [options]
    
Commands:
    list                    - List all users
    add <username>          - Add a new user (prompts for password)
    password <username>     - Reset user password (prompts for new password)
    disable-2fa <username>  - Disable 2FA for a user
    delete <username>       - Delete a user
    info <username>         - Show user information
"""

import os
import sys
import argparse
import hashlib
import getpass
from datetime import datetime
from pysqlcipher3 import dbapi2 as sqlite

# Configuration - should match app.py
DB_PATH = '/home/klapvogn/apps/znc_search/znc_logs.db'
DB_KEY = '28ab2972b162ccc779d905cb6b422cd707d0470aef68c4289b41fa8ea42fb7df'  # Must match app.py

def get_db():
    """Get database connection with encryption"""
    if not os.path.exists(DB_PATH):
        print(f"Error: Database not found: {DB_PATH}")
        sys.exit(1)
    
    conn = sqlite.connect(DB_PATH)
    conn.execute(f"PRAGMA key = '{DB_KEY}'")
    conn.execute("PRAGMA cipher_compatibility = 4")
    return conn

def hash_password(password):
    """Hash password using SHA256"""
    return hashlib.sha256(password.encode()).hexdigest()

def list_users():
    """List all users"""
    conn = get_db()
    cursor = conn.cursor()
    
    cursor.execute('''
        SELECT id, username, totp_enabled, created_at, updated_at
        FROM users
        ORDER BY username
    ''')
    
    users = cursor.fetchall()
    conn.close()
    
    if not users:
        print("No users found in database.")
        return
    
    print("\n" + "=" * 80)
    print("USERS")
    print("=" * 80)
    print(f"{'ID':<5} {'Username':<20} {'2FA':<10} {'Created':<20} {'Updated':<20}")
    print("-" * 80)
    
    for user in users:
        user_id, username, totp_enabled, created, updated = user
        tfa_status = "Enabled" if totp_enabled else "Disabled"
        print(f"{user_id:<5} {username:<20} {tfa_status:<10} {created:<20} {updated:<20}")
    
    print()

def add_user(username):
    """Add a new user"""
    if not username:
        print("Error: Username is required")
        return False
    
    # Get password with confirmation
    while True:
        password = getpass.getpass("Enter password (min 8 characters): ")
        if len(password) < 8:
            print("Error: Password must be at least 8 characters")
            continue
        
        confirm = getpass.getpass("Confirm password: ")
        if password != confirm:
            print("Error: Passwords do not match")
            continue
        
        break
    
    password_hash = hash_password(password)
    
    conn = get_db()
    cursor = conn.cursor()
    
    try:
        cursor.execute('''
            INSERT INTO users (username, password_hash, totp_enabled)
            VALUES (?, ?, 0)
        ''', (username, password_hash))
        
        conn.commit()
        print(f"\n✓ User '{username}' created successfully")
        
    except sqlite.IntegrityError:
        print(f"\n✗ Error: Username '{username}' already exists")
        conn.close()
        return False
    except Exception as e:
        print(f"\n✗ Error creating user: {e}")
        conn.close()
        return False
    
    conn.close()
    return True

def reset_password(username):
    """Reset user password"""
    if not username:
        print("Error: Username is required")
        return False
    
    conn = get_db()
    cursor = conn.cursor()
    
    # Check if user exists
    cursor.execute('SELECT id FROM users WHERE username = ?', (username,))
    if not cursor.fetchone():
        print(f"\n✗ Error: User '{username}' not found")
        conn.close()
        return False
    
    # Get new password with confirmation
    while True:
        password = getpass.getpass("Enter new password (min 8 characters): ")
        if len(password) < 8:
            print("Error: Password must be at least 8 characters")
            continue
        
        confirm = getpass.getpass("Confirm new password: ")
        if password != confirm:
            print("Error: Passwords do not match")
            continue
        
        break
    
    password_hash = hash_password(password)
    
    try:
        cursor.execute('''
            UPDATE users 
            SET password_hash = ?, updated_at = CURRENT_TIMESTAMP
            WHERE username = ?
        ''', (password_hash, username))
        
        conn.commit()
        print(f"\n✓ Password for user '{username}' reset successfully")
        
    except Exception as e:
        print(f"\n✗ Error resetting password: {e}")
        conn.close()
        return False
    
    conn.close()
    return True

def disable_2fa(username):
    """Disable 2FA for a user"""
    if not username:
        print("Error: Username is required")
        return False
    
    conn = get_db()
    cursor = conn.cursor()
    
    # Check if user exists
    cursor.execute('SELECT id, totp_enabled FROM users WHERE username = ?', (username,))
    result = cursor.fetchone()
    
    if not result:
        print(f"\n✗ Error: User '{username}' not found")
        conn.close()
        return False
    
    user_id, totp_enabled = result
    
    if not totp_enabled:
        print(f"\n⚠ 2FA is already disabled for user '{username}'")
        conn.close()
        return True
    
    try:
        cursor.execute('''
            UPDATE users 
            SET totp_enabled = 0, totp_secret = NULL, updated_at = CURRENT_TIMESTAMP
            WHERE username = ?
        ''', (username,))
        
        conn.commit()
        print(f"\n✓ 2FA disabled for user '{username}'")
        
    except Exception as e:
        print(f"\n✗ Error disabling 2FA: {e}")
        conn.close()
        return False
    
    conn.close()
    return True

def delete_user(username):
    """Delete a user"""
    if not username:
        print("Error: Username is required")
        return False
    
    # Confirm deletion
    confirm = input(f"Are you sure you want to delete user '{username}'? (yes/no): ")
    if confirm.lower() not in ['yes', 'y']:
        print("Deletion cancelled")
        return False
    
    conn = get_db()
    cursor = conn.cursor()
    
    try:
        cursor.execute('DELETE FROM users WHERE username = ?', (username,))
        
        if cursor.rowcount == 0:
            print(f"\n✗ Error: User '{username}' not found")
            conn.close()
            return False
        
        conn.commit()
        print(f"\n✓ User '{username}' deleted successfully")
        
    except Exception as e:
        print(f"\n✗ Error deleting user: {e}")
        conn.close()
        return False
    
    conn.close()
    return True

def user_info(username):
    """Show detailed user information"""
    if not username:
        print("Error: Username is required")
        return False
    
    conn = get_db()
    cursor = conn.cursor()
    
    cursor.execute('''
        SELECT id, username, totp_enabled, totp_secret, created_at, updated_at
        FROM users
        WHERE username = ?
    ''', (username,))
    
    result = cursor.fetchone()
    conn.close()
    
    if not result:
        print(f"\n✗ Error: User '{username}' not found")
        return False
    
    user_id, username, totp_enabled, totp_secret, created, updated = result
    
    print("\n" + "=" * 60)
    print("USER INFORMATION")
    print("=" * 60)
    print(f"ID:              {user_id}")
    print(f"Username:        {username}")
    print(f"2FA Status:      {'Enabled' if totp_enabled else 'Disabled'}")
    print(f"2FA Secret:      {'[SET]' if totp_secret else '[NOT SET]'}")
    print(f"Created:         {created}")
    print(f"Last Updated:    {updated}")
    print()
    
    return True

def main():
    parser = argparse.ArgumentParser(description='IRC Log Search User Management')
    parser.add_argument('command', 
                       choices=['list', 'add', 'password', 'disable-2fa', 'delete', 'info'],
                       help='Command to execute')
    parser.add_argument('username', nargs='?',
                       help='Username (required for most commands)')
    
    args = parser.parse_args()
    
    if args.command == 'list':
        list_users()
    elif args.command == 'add':
        if not args.username:
            print("Error: Username is required")
            sys.exit(1)
        add_user(args.username)
    elif args.command == 'password':
        if not args.username:
            print("Error: Username is required")
            sys.exit(1)
        reset_password(args.username)
    elif args.command == 'disable-2fa':
        if not args.username:
            print("Error: Username is required")
            sys.exit(1)
        disable_2fa(args.username)
    elif args.command == 'delete':
        if not args.username:
            print("Error: Username is required")
            sys.exit(1)
        delete_user(args.username)
    elif args.command == 'info':
        if not args.username:
            print("Error: Username is required")
            sys.exit(1)
        user_info(args.username)

if __name__ == '__main__':
    main()