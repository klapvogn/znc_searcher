#!/usr/bin/env python3
"""
ZNC Log Search - Flask app with MySQL backend
"""

from flask import Flask, request, jsonify, render_template, session, redirect, url_for
from flask_cors import CORS
import os
import re
import sys
import hashlib
from functools import wraps
import mysql.connector
from mysql.connector import pooling
from dotenv import load_dotenv
import getpass
import pyotp
import qrcode
import io
import base64

load_dotenv()

app = Flask(__name__)
app.secret_key = os.getenv('SECRET_KEY')
CORS(app)

# Configuration
MYSQL_HOST = os.getenv('MYSQL_HOST', 'localhost')
MYSQL_PORT = int(os.getenv('MYSQL_PORT', 3306))
MYSQL_USER = os.getenv('MYSQL_USER', 'root')
MYSQL_PASSWORD = os.getenv('MYSQL_PASSWORD', '')
MYSQL_DATABASE = os.getenv('MYSQL_DATABASE', 'znc_logs')

if not app.secret_key:
    print("Error: SECRET_KEY not found in environment variables")
    sys.exit(1)

# Connection pool
db_pool = pooling.MySQLConnectionPool(
    pool_name="znc_pool",
    pool_size=5,
    host=MYSQL_HOST,
    port=MYSQL_PORT,
    user=MYSQL_USER,
    password=MYSQL_PASSWORD,
    database=MYSQL_DATABASE,
    charset='utf8mb4',
    collation='utf8mb4_unicode_ci'
)


def get_db():
    """Get a connection from the pool"""
    return db_pool.get_connection()


def hash_password(password):
    """Hash password using SHA256"""
    return hashlib.sha256(password.encode()).hexdigest()


def init_db():
    """Create default admin user if not exists"""
    conn = get_db()
    cursor = conn.cursor()

    cursor.execute('SELECT COUNT(*) FROM users WHERE username = %s', ('admin',))
    if cursor.fetchone()[0] == 0:
        default_hash = hash_password('admin')
        cursor.execute(
            'INSERT INTO users (username, password_hash, totp_enabled) VALUES (%s, %s, 0)',
            ('admin', default_hash)
        )
        conn.commit()
        print("WARNING: Default admin user created with password 'admin'. Please change it immediately!")

    cursor.close()
    conn.close()


def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'logged_in' not in session:
            return jsonify({'error': 'Not authenticated'}), 401
        return f(*args, **kwargs)
    return decorated_function


# ─── Auth routes ──────────────────────────────────────────────────────────────

@app.route('/')
def index():
    if 'logged_in' not in session:
        return redirect(url_for('login_page'))
    return render_template('index.html')


@app.route('/login')
def login_page():
    if 'logged_in' in session:
        return redirect(url_for('index'))
    return render_template('login.html')


@app.route('/api/login', methods=['POST'])
def login():
    data = request.json
    username = data.get('username')
    password = data.get('password')
    totp_code = data.get('totp_code')

    if not username or not password:
        return jsonify({'error': 'Username and password required'}), 400

    conn = get_db()
    cursor = conn.cursor()

    cursor.execute(
        'SELECT id, username, password_hash, totp_secret, totp_enabled '
        'FROM users WHERE username = %s',
        (username,)
    )
    user = cursor.fetchone()
    cursor.close()
    conn.close()

    if not user:
        return jsonify({'error': 'Invalid credentials'}), 401

    user_id, username, password_hash, totp_secret, totp_enabled = user

    if hash_password(password) != password_hash:
        return jsonify({'error': 'Invalid credentials'}), 401

    if totp_enabled:
        if not totp_code:
            return jsonify({'requires_2fa': True, 'message': 'Please enter your 2FA code'}), 200
        totp = pyotp.TOTP(totp_secret)
        if not totp.verify(totp_code, valid_window=1):
            return jsonify({'error': 'Invalid 2FA code'}), 401

    session['logged_in'] = True
    session['username'] = username
    session['user_id'] = user_id

    return jsonify({'success': True, 'redirect': '/'})


@app.route('/api/logout', methods=['POST'])
def logout():
    session.clear()
    return jsonify({'success': True})


# ─── User routes ──────────────────────────────────────────────────────────────

@app.route('/api/user/info', methods=['GET'])
@login_required
def get_user_info():
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute('SELECT username, totp_enabled FROM users WHERE id = %s', (session['user_id'],))
    result = cursor.fetchone()
    cursor.close()
    conn.close()

    if not result:
        return jsonify({'error': 'User not found'}), 404

    return jsonify({'username': result[0], 'two_factor_enabled': bool(result[1])})


@app.route('/api/user/password', methods=['POST'])
@login_required
def change_password():
    data = request.json
    current_password = data.get('current_password')
    new_password = data.get('new_password')

    if not current_password or not new_password:
        return jsonify({'error': 'Current and new password required'}), 400

    if len(new_password) < 8:
        return jsonify({'error': 'New password must be at least 8 characters'}), 400

    conn = get_db()
    cursor = conn.cursor()
    cursor.execute('SELECT password_hash FROM users WHERE id = %s', (session['user_id'],))
    user = cursor.fetchone()

    if not user or hash_password(current_password) != user[0]:
        cursor.close()
        conn.close()
        return jsonify({'error': 'Current password is incorrect'}), 401

    cursor.execute(
        'UPDATE users SET password_hash = %s WHERE id = %s',
        (hash_password(new_password), session['user_id'])
    )
    conn.commit()
    cursor.close()
    conn.close()

    return jsonify({'success': True, 'message': 'Password changed successfully'})


@app.route('/api/user/2fa/status', methods=['GET'])
@login_required
def get_2fa_status():
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute('SELECT totp_enabled FROM users WHERE id = %s', (session['user_id'],))
    result = cursor.fetchone()
    cursor.close()
    conn.close()
    return jsonify({'enabled': bool(result[0]) if result else False})


@app.route('/api/user/2fa/setup', methods=['POST'])
@login_required
def setup_2fa():
    secret = pyotp.random_base32()

    conn = get_db()
    cursor = conn.cursor()
    cursor.execute(
        'UPDATE users SET totp_secret = %s WHERE id = %s',
        (secret, session['user_id'])
    )
    conn.commit()
    cursor.execute('SELECT username FROM users WHERE id = %s', (session['user_id'],))
    username = cursor.fetchone()[0]
    cursor.close()
    conn.close()

    totp_uri = pyotp.totp.TOTP(secret).provisioning_uri(name=username, issuer_name='IRC Log Search')
    qr = qrcode.QRCode(version=1, box_size=10, border=5)
    qr.add_data(totp_uri)
    qr.make(fit=True)
    img = qr.make_image(fill_color="black", back_color="white")
    buffer = io.BytesIO()
    img.save(buffer, format='PNG')
    img_str = base64.b64encode(buffer.getvalue()).decode()

    return jsonify({'secret': secret, 'qr_code': f'data:image/png;base64,{img_str}', 'manual_entry': secret})


@app.route('/api/user/2fa/enable', methods=['POST'])
@login_required
def enable_2fa():
    data = request.json
    totp_code = data.get('code')

    if not totp_code:
        return jsonify({'error': 'Verification code required'}), 400

    conn = get_db()
    cursor = conn.cursor()
    cursor.execute('SELECT totp_secret FROM users WHERE id = %s', (session['user_id'],))
    result = cursor.fetchone()

    if not result or not result[0]:
        cursor.close()
        conn.close()
        return jsonify({'error': 'Please setup 2FA first'}), 400

    if not pyotp.TOTP(result[0]).verify(totp_code, valid_window=1):
        cursor.close()
        conn.close()
        return jsonify({'error': 'Invalid verification code'}), 401

    cursor.execute('UPDATE users SET totp_enabled = 1 WHERE id = %s', (session['user_id'],))
    conn.commit()
    cursor.close()
    conn.close()

    return jsonify({'success': True, 'message': '2FA enabled successfully'})


@app.route('/api/user/2fa/disable', methods=['POST'])
@login_required
def disable_2fa():
    data = request.json
    password = data.get('password')

    if not password:
        return jsonify({'error': 'Password required'}), 400

    conn = get_db()
    cursor = conn.cursor()
    cursor.execute('SELECT password_hash FROM users WHERE id = %s', (session['user_id'],))
    result = cursor.fetchone()

    if not result or hash_password(password) != result[0]:
        cursor.close()
        conn.close()
        return jsonify({'error': 'Invalid password'}), 401

    cursor.execute(
        'UPDATE users SET totp_enabled = 0, totp_secret = NULL WHERE id = %s',
        (session['user_id'],)
    )
    conn.commit()
    cursor.close()
    conn.close()

    return jsonify({'success': True, 'message': '2FA disabled successfully'})


# ─── Log routes ───────────────────────────────────────────────────────────────

@app.route('/api/networks', methods=['GET'])
@login_required
def get_networks():
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute('''
        SELECT DISTINCT n.id, n.display_name 
        FROM networks n
        INNER JOIN log_entries le ON n.id = le.network_id
        ORDER BY n.display_name
    ''')
    networks = [{'id': row[0], 'name': row[1]} for row in cursor.fetchall()]
    cursor.close()
    conn.close()
    return jsonify({'networks': networks})


@app.route('/api/channels/<network>', methods=['GET'])
@login_required
def get_channels(network):
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute('''
        SELECT DISTINCT channel_name 
        FROM log_entries 
        WHERE network_id = %s AND channel_name LIKE '#%%'
        ORDER BY channel_name
    ''', (network,))
    channels = [row[0] for row in cursor.fetchall()]
    cursor.close()
    conn.close()
    return jsonify({'channels': channels})


@app.route('/api/search', methods=['POST'])
@login_required
def search_logs():
    data = request.json
    query = data.get('query', '').strip()
    network = data.get('network', '')
    channel = data.get('channel', '')
    start_date = data.get('start_date')
    end_date = data.get('end_date')

    if not query:
        return jsonify({'error': 'Query required'}), 400
    if not network:
        return jsonify({'error': 'Network required'}), 400

    conn = get_db()
    cursor = conn.cursor()

    # Single plain word → FULLTEXT (fast, indexed).
    # Phrases, URLs, sentences, special chars → LIKE (exact substring).
    use_fulltext = bool(re.match(r'^\w+$', query))

    if use_fulltext:
        sql = '''
            SELECT 
                le.network_id,
                n.display_name,
                le.channel_name,
                le.log_date,
                le.line_number,
                le.content
            FROM log_entries le
            JOIN networks n ON le.network_id = n.id
            WHERE le.network_id = %s
            AND MATCH(le.content) AGAINST (%s IN BOOLEAN MODE)
        '''
        params = [network, query]
    else:
        sql = '''
            SELECT 
                le.network_id,
                n.display_name,
                le.channel_name,
                le.log_date,
                le.line_number,
                le.content
            FROM log_entries le
            JOIN networks n ON le.network_id = n.id
            WHERE le.network_id = %s
            AND le.content LIKE %s
        '''
        params = [network, f'%{query}%']

    if channel:
        sql += ' AND le.channel_name = %s'
        params.append(channel)

    if start_date:
        sql += ' AND le.log_date >= %s'
        params.append(start_date)

    if end_date:
        sql += ' AND le.log_date <= %s'
        params.append(end_date)

    sql += ' ORDER BY le.log_date DESC, le.line_number ASC LIMIT 1000'

    cursor.execute(sql, params)

    results = []
    for row in cursor.fetchall():
        results.append({
            'network_id': row[0],
            'network': row[1],
            'channel': row[2],
            'date': str(row[3]),
            'line': row[4],
            'content': row[5]
        })

    cursor.close()
    conn.close()

    return jsonify({
        'results': results,
        'total': len(results),
        'truncated': len(results) >= 1000
    })


@app.route('/api/context', methods=['POST'])
@login_required
def get_context():
    data = request.json
    network = data.get('network')
    channel = data.get('channel')
    log_date = data.get('date')
    center_line = data.get('line')
    lines_before = data.get('lines_before', 2)
    lines_after = data.get('lines_after', 2)

    if not all([network, channel, log_date, center_line]):
        return jsonify({'error': 'Missing required parameters'}), 400

    conn = get_db()
    cursor = conn.cursor()

    start_line = max(1, center_line - lines_before)
    end_line = center_line + lines_after

    cursor.execute('''
        SELECT line_number, content
        FROM log_entries
        WHERE network_id = %s AND channel_name = %s AND log_date = %s
        AND line_number BETWEEN %s AND %s
        ORDER BY line_number
    ''', (network, channel, log_date, start_line, end_line))

    context = [
        {'line': row[0], 'content': row[1], 'is_match': row[0] == center_line}
        for row in cursor.fetchall()
    ]

    cursor.execute('''
        SELECT COUNT(*) FROM log_entries
        WHERE network_id = %s AND channel_name = %s AND log_date = %s
    ''', (network, channel, log_date))
    total_lines = cursor.fetchone()[0]

    cursor.close()
    conn.close()

    return jsonify({
        'context': context,
        'start_line': start_line,
        'end_line': end_line,
        'total_lines': total_lines,
        'can_expand_up': start_line > 1,
        'can_expand_down': end_line < total_lines
    })


@app.route('/api/stats', methods=['GET'])
@login_required
def get_stats():
    conn = get_db()
    cursor = conn.cursor()

    cursor.execute('SELECT COUNT(*) FROM log_entries')
    total_entries = cursor.fetchone()[0]

    cursor.execute('SELECT MIN(log_date), MAX(log_date) FROM log_entries')
    date_range = cursor.fetchone()

    cursor.execute('SELECT COUNT(DISTINCT network_id) FROM log_entries')
    network_count = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(DISTINCT channel_name) FROM log_entries WHERE channel_name LIKE '#%'")
    channel_count = cursor.fetchone()[0]

    cursor.execute('''
        SELECT n.display_name, COUNT(*) 
        FROM log_entries le
        JOIN networks n ON le.network_id = n.id
        GROUP BY n.display_name
        ORDER BY COUNT(*) DESC
    ''')
    network_stats = [{'network': row[0], 'count': row[1]} for row in cursor.fetchall()]

    cursor.close()
    conn.close()

    return jsonify({
        'total_entries': total_entries,
        'network_count': network_count,
        'channel_count': channel_count,
        'date_range': {'start': str(date_range[0]) if date_range[0] else None,
                       'end': str(date_range[1]) if date_range[1] else None},
        'networks': network_stats
    })


if __name__ == '__main__':
    init_db()
    app.run(host='0.0.0.0', port=5000, debug=False)