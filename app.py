import sqlite3
import os
import collections
import re
from datetime import datetime
from flask import Flask, render_template, request, session, jsonify, redirect, url_for, send_from_directory
import pandas as pd

# --- ЗАПЛАТКА ДЛЯ LDAP3 ---
if not hasattr(collections, 'MutableMapping'):
    import collections.abc

    collections.MutableMapping = collections.abc.MutableMapping
if not hasattr(collections, 'Sequence'):
    import collections.abc

    collections.Sequence = collections.abc.Sequence

from ldap3 import Server, Connection, ALL, SUBTREE

app = Flask(__name__)
app.secret_key = 'enterprise_hub_production_v37'

LDAP_CONFIG = {
    'uri': "ldap://192.168.0.4",
    'base': "DC=local,DC=energoprom,DC=by",
    'bind_dn': "CN=OC1,OU=ОЦ,DC=local,DC=energoprom,DC=by",
    'bind_password': "Pass5678",
    'user_attr': "sAMAccountName"
}

MASTER_ADMINS = ['rapeiko', 'oc1']
DB_PATH = 'database.db'
LOGO_FILENAME = 'image2_hq.png'
PHONEBOOK_PATH = 'phonebook.xlsx'
PHONEBOOK_COLUMNS = ['dept', 'pos', 'surname', 'name', 'work', 'home', 'mobile']
_phonebook_cache = None
_phonebook_mtime = None


def get_db_connection():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.execute('PRAGMA journal_mode=WAL;')
    conn.row_factory = sqlite3.Row
    return conn


def format_phone(phone):
    source = str(phone).replace('.0', '').strip()
    digits = re.sub(r'\D', '', source)
    if len(digits) == 12:
        return f"+{digits[:3]}-{digits[3:5]}-{digits[5:8]}-{digits[8:10]}-{digits[10:]}"
    return source


def get_phonebook_contacts():
    global _phonebook_cache, _phonebook_mtime
    if not os.path.exists(PHONEBOOK_PATH):
        return []

    current_mtime = os.path.getmtime(PHONEBOOK_PATH)
    if _phonebook_cache is not None and _phonebook_mtime == current_mtime:
        return _phonebook_cache

    df = pd.read_excel(PHONEBOOK_PATH, names=PHONEBOOK_COLUMNS)
    df['work'] = df['work'].apply(format_phone)
    df['mobile'] = df['mobile'].apply(format_phone)
    df['home'] = df['home'].apply(format_phone)

    _phonebook_cache = df.fillna('').to_dict(orient='records')
    _phonebook_mtime = current_mtime
    return _phonebook_cache


def init_db():
    with get_db_connection() as conn:
        # 1. Проверяем/Обновляем таблицу ресурсов (удаляем старый столбец access_group_id если он мешает)
        conn.execute('''
            CREATE TABLE IF NOT EXISTS resources (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                title TEXT NOT NULL, url TEXT NOT NULL,
                category TEXT NOT NULL, desc TEXT, 
                position INTEGER DEFAULT 0
            )''')

        # 2. Создаем новую таблицу связей (МНОГИЕ-КО-МНОГИМ)
        conn.execute('''
            CREATE TABLE IF NOT EXISTS resource_group_access (
                resource_id INTEGER,
                group_id INTEGER,
                PRIMARY KEY (resource_id, group_id)
            )''')

        # 3. Группы
        conn.execute('CREATE TABLE IF NOT EXISTS groups (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT UNIQUE)')

        # 3.1 Разделы (категории ресурсов)
        conn.execute('CREATE TABLE IF NOT EXISTS categories (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT UNIQUE)')

        # 4. Участники
        conn.execute('''
            CREATE TABLE IF NOT EXISTS group_members (
                group_id INTEGER, username TEXT, 
                PRIMARY KEY (group_id, username)
            )''')
        # 5. Переговорки
        conn.execute('''
            CREATE TABLE IF NOT EXISTS meeting_rooms (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT UNIQUE NOT NULL
            )''')

        # 6. Брони переговорок
        conn.execute('''
            CREATE TABLE IF NOT EXISTS meeting_bookings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                room_id INTEGER NOT NULL,
                booked_by TEXT NOT NULL,
                purpose TEXT NOT NULL,
                meeting_date TEXT NOT NULL,
                start_time TEXT NOT NULL,
                end_time TEXT NOT NULL,
                owner_username TEXT NOT NULL,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(room_id) REFERENCES meeting_rooms(id)
            )''')
        # Синхронизируем справочник разделов с уже существующими ресурсами
        existing_categories = conn.execute(
            "SELECT DISTINCT TRIM(category) AS category FROM resources WHERE TRIM(category) <> ''"
        ).fetchall()
        for row in existing_categories:
            conn.execute('INSERT OR IGNORE INTO categories (name) VALUES (?)', (row['category'],))
        default_rooms = ['Переговорка 1', 'Переговорка 2', 'Конференц-зал']
        for room_name in default_rooms:
            conn.execute('INSERT OR IGNORE INTO meeting_rooms (name) VALUES (?)', (room_name,))
        conn.commit()
def get_user_ad_groups(conn, user_dn):
    search_filter = f"(member:1.2.840.113556.1.4.1941:={user_dn})"
    conn.search(LDAP_CONFIG['base'], search_filter, SUBTREE, attributes=['sAMAccountName', 'cn'])
    groups = []
    for entry in conn.entries:
        if entry.sAMAccountName: groups.append(str(entry.sAMAccountName).lower())
        if entry.cn: groups.append(str(entry.cn).lower())
    return list(set(groups))


def check_ldap_auth(username, password):
    try:
        server = Server(LDAP_CONFIG['uri'], get_info=ALL, connect_timeout=5)
        conn = Connection(server, user=LDAP_CONFIG['bind_dn'], password=LDAP_CONFIG['bind_password'], auto_bind=True)
        search_filter = f"({LDAP_CONFIG['user_attr']}={username})"
        conn.search(LDAP_CONFIG['base'], search_filter, SUBTREE, attributes=['distinguishedName'])
        if not conn.entries: return False
        user_dn = conn.entries[0].distinguishedName.value
        user_conn = Connection(server, user=user_dn, password=password, auto_bind=True)
        session['ad_groups'] = get_user_ad_groups(conn, user_dn)
        return True
    except:
        return False


@app.route('/')
def index():
    if not session.get('logged_in'): return redirect(url_for('login_page'))
    is_admin = session.get('username') in MASTER_ADMINS
    return render_template('index.html', is_admin=is_admin, user=session.get('username'))


@app.route('/login', methods=['GET', 'POST'])
def login_page():
    if request.method == 'GET':
        if session.get('logged_in'): return redirect(url_for('index'))
        return render_template('login.html')
    data = request.json
    u, p = data.get('username', '').strip().lower(), data.get('password', '')
    if check_ldap_auth(u, p):
        session['logged_in'] = True
        session['username'] = u
        return jsonify(success=True)
    return jsonify(success=False), 401


@app.route('/manage')
def manage_page():
    if not session.get('logged_in') or session.get('username') not in MASTER_ADMINS:
        return redirect(url_for('index'))
    return render_template('manage.html', user=session.get('username'))


@app.route('/manage/categories')
def manage_categories_page():
    if not session.get('logged_in') or session.get('username') not in MASTER_ADMINS:
        return redirect(url_for('index'))
    return render_template('manage_categories.html', user=session.get('username'))


@app.route('/phonebook')
def phonebook_page():
    if not session.get('logged_in'):
        return redirect(url_for('login_page'))
    is_admin = session.get('username') in MASTER_ADMINS
    contacts = get_phonebook_contacts()
    return render_template('phonebook.html', is_admin=is_admin, user=session.get('username'), contacts=contacts)


@app.route('/meeting-rooms')
def meeting_rooms_page():
    if not session.get('logged_in'):
        return redirect(url_for('login_page'))
    is_admin = session.get('username') in MASTER_ADMINS
    return render_template('meeting_rooms.html', is_admin=is_admin, user=session.get('username'))


@app.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('login_page'))


@app.route('/api/meeting-rooms')
def get_meeting_rooms():
    if not session.get('logged_in'):
        return jsonify([]), 403
    conn = get_db_connection()
    try:
        rows = conn.execute('SELECT id, name FROM meeting_rooms ORDER BY name COLLATE NOCASE').fetchall()
        return jsonify([dict(r) for r in rows])
    finally:
        conn.close()


@app.route('/api/meeting-rooms', methods=['POST'])
def create_meeting_room():
    if session.get('username') not in MASTER_ADMINS:
        return jsonify(success=False, error='Только администратор может добавлять переговорки'), 403
    data = request.json or {}
    room_name = (data.get('name') or '').strip()
    if not room_name:
        return jsonify(success=False, error='Укажите название переговорки'), 400
    conn = get_db_connection()
    try:
        exists = conn.execute('SELECT 1 FROM meeting_rooms WHERE name = ?', (room_name,)).fetchone()
        if exists:
            return jsonify(success=False, error='Такая переговорка уже существует'), 409
        conn.execute('INSERT INTO meeting_rooms (name) VALUES (?)', (room_name,))
        conn.commit()
        return jsonify(success=True)
    finally:
        conn.close()


@app.route('/api/meeting-rooms/<int:room_id>', methods=['DELETE'])
def delete_meeting_room(room_id):
    if session.get('username') not in MASTER_ADMINS:
        return jsonify(success=False, error='Только администратор может удалять переговорки'), 403
    conn = get_db_connection()
    try:
        has_bookings = conn.execute('SELECT 1 FROM meeting_bookings WHERE room_id = ? LIMIT 1', (room_id,)).fetchone()
        if has_bookings:
            return jsonify(success=False, error='Нельзя удалить переговорку: есть существующие брони'), 409
        conn.execute('DELETE FROM meeting_rooms WHERE id = ?', (room_id,))
        conn.commit()
        return jsonify(success=True)
    finally:
        conn.close()


def _meeting_has_conflict(conn, booking_payload, booking_id=None):
    params = [
        booking_payload['room_id'],
        booking_payload['meeting_date'],
        booking_payload['end_time'],
        booking_payload['start_time']
    ]
    query = '''
        SELECT 1
        FROM meeting_bookings
        WHERE room_id = ?
          AND meeting_date = ?
          AND NOT (? <= start_time OR ? >= end_time)
    '''
    if booking_id is not None:
        query += ' AND id <> ?'
        params.append(booking_id)
    query += ' LIMIT 1'
    return conn.execute(query, tuple(params)).fetchone() is not None


def _is_booking_in_past(payload):
    try:
        meeting_start = datetime.strptime(
            f"{payload['meeting_date']} {payload['start_time']}",
            "%Y-%m-%d %H:%M"
        )
    except (TypeError, ValueError):
        return False
    return meeting_start < datetime.now()


@app.route('/api/meeting-bookings')
def get_meeting_bookings():
    if not session.get('logged_in'):
        return jsonify([]), 403
    conn = get_db_connection()
    try:
        rows = conn.execute('''
            SELECT
                b.id, b.room_id, r.name AS room_name, b.booked_by, b.purpose,
                b.meeting_date, b.start_time, b.end_time, b.owner_username
            FROM meeting_bookings b
            JOIN meeting_rooms r ON r.id = b.room_id
            ORDER BY b.meeting_date ASC, b.start_time ASC
        ''').fetchall()
        return jsonify([dict(r) for r in rows])
    finally:
        conn.close()


@app.route('/api/meeting-bookings', methods=['POST'])
def create_meeting_booking():
    if not session.get('logged_in'):
        return jsonify(success=False), 403
    data = request.json or {}
    payload = {
        'room_id': data.get('room_id'),
        'booked_by': (data.get('booked_by') or '').strip(),
        'purpose': (data.get('purpose') or '').strip(),
        'meeting_date': (data.get('meeting_date') or '').strip(),
        'start_time': (data.get('start_time') or '').strip(),
        'end_time': (data.get('end_time') or '').strip(),
    }
    if not all([payload['room_id'], payload['booked_by'], payload['purpose'], payload['meeting_date'],
                payload['start_time'], payload['end_time']]):
        return jsonify(success=False, error='Заполните все поля бронирования'), 400
    if payload['end_time'] <= payload['start_time']:
        return jsonify(success=False, error='Время окончания должно быть больше времени начала'), 400
    if _is_booking_in_past(payload):
        return jsonify(success=False, error='Нельзя создавать бронь на прошедшие дату и время'), 400
    conn = get_db_connection()
    try:
        room_exists = conn.execute('SELECT 1 FROM meeting_rooms WHERE id = ?', (payload['room_id'],)).fetchone()
        if not room_exists:
            return jsonify(success=False, error='Выбранная переговорка не найдена'), 404
        if _meeting_has_conflict(conn, payload):
            return jsonify(success=False, error='На выбранное время переговорка уже занята'), 409
        conn.execute('''
            INSERT INTO meeting_bookings (
                room_id, booked_by, purpose, meeting_date, start_time, end_time, owner_username
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (
            payload['room_id'], payload['booked_by'], payload['purpose'], payload['meeting_date'],
            payload['start_time'], payload['end_time'], session.get('username')
        ))
        conn.commit()
        return jsonify(success=True)
    finally:
        conn.close()


@app.route('/api/meeting-bookings/<int:booking_id>', methods=['PUT'])
def update_meeting_booking(booking_id):
    if not session.get('logged_in'):
        return jsonify(success=False), 403
    data = request.json or {}
    payload = {
        'room_id': data.get('room_id'),
        'booked_by': (data.get('booked_by') or '').strip(),
        'purpose': (data.get('purpose') or '').strip(),
        'meeting_date': (data.get('meeting_date') or '').strip(),
        'start_time': (data.get('start_time') or '').strip(),
        'end_time': (data.get('end_time') or '').strip(),
    }
    if not all([payload['room_id'], payload['booked_by'], payload['purpose'], payload['meeting_date'],
                payload['start_time'], payload['end_time']]):
        return jsonify(success=False, error='Заполните все поля бронирования'), 400
    if payload['end_time'] <= payload['start_time']:
        return jsonify(success=False, error='Время окончания должно быть больше времени начала'), 400
    if _is_booking_in_past(payload):
        return jsonify(success=False, error='Нельзя сохранять бронь на прошедшие дату и время'), 400
    conn = get_db_connection()
    try:
        booking = conn.execute('SELECT owner_username FROM meeting_bookings WHERE id = ?', (booking_id,)).fetchone()
        if not booking:
            return jsonify(success=False, error='Бронь не найдена'), 404
        current_user = session.get('username')
        if current_user != 'oc1':
            return jsonify(success=False, error='Редактирование доступно только администратору oc1'), 403
        if _meeting_has_conflict(conn, payload, booking_id=booking_id):
            return jsonify(success=False, error='На выбранное время переговорка уже занята'), 409
        conn.execute('''
            UPDATE meeting_bookings
            SET room_id = ?, booked_by = ?, purpose = ?, meeting_date = ?, start_time = ?, end_time = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
        ''', (
            payload['room_id'], payload['booked_by'], payload['purpose'], payload['meeting_date'],
            payload['start_time'], payload['end_time'], booking_id
        ))
        conn.commit()
        return jsonify(success=True)
    finally:
        conn.close()


@app.route('/api/meeting-bookings/<int:booking_id>', methods=['DELETE'])
def delete_meeting_booking(booking_id):
    if not session.get('logged_in'):
        return jsonify(success=False), 403
    conn = get_db_connection()
    try:
        booking = conn.execute('SELECT owner_username FROM meeting_bookings WHERE id = ?', (booking_id,)).fetchone()
        if not booking:
            return jsonify(success=False, error='Бронь не найдена'), 404
        current_user = session.get('username')
        if current_user != 'oc1' and booking['owner_username'] != current_user:
            return jsonify(success=False, error='Можно удалять только свои брони'), 403
        conn.execute('DELETE FROM meeting_bookings WHERE id = ?', (booking_id,))
        conn.commit()
        return jsonify(success=True)
    finally:
        conn.close()


@app.route('/assets/logo')
def project_logo():
    return send_from_directory(app.root_path, LOGO_FILENAME)


@app.route('/assets/<path:filename>')
def project_asset(filename):
    return send_from_directory(app.root_path, filename)


@app.route('/get_groups')
def get_groups():
    if not session.get('logged_in'): return jsonify([])
    conn = get_db_connection()
    try:
        groups = conn.execute('SELECT * FROM groups').fetchall()
        res = []
        for g in groups:
            m = conn.execute('SELECT username FROM group_members WHERE group_id = ?', (g['id'],)).fetchall()
            res.append({'id': g['id'], 'name': g['name'], 'members': [x['username'] for x in m]})
        return jsonify(res)
    finally:
        conn.close()


@app.route('/get_categories')
def get_categories():
    if not session.get('logged_in'):
        return jsonify([])
    conn = get_db_connection()
    try:
        rows = conn.execute('SELECT name FROM categories ORDER BY name COLLATE NOCASE').fetchall()
        return jsonify([r['name'] for r in rows])
    finally:
        conn.close()


@app.route('/get_categories_overview')
def get_categories_overview():
    if session.get('username') not in MASTER_ADMINS:
        return jsonify([]), 403
    conn = get_db_connection()
    try:
        rows = conn.execute('''
            SELECT c.name AS name, COUNT(r.id) AS resource_count
            FROM categories c
            LEFT JOIN resources r ON TRIM(r.category) = c.name
            GROUP BY c.name
            ORDER BY c.name COLLATE NOCASE
        ''').fetchall()
        return jsonify([{'name': r['name'], 'resource_count': r['resource_count']} for r in rows])
    finally:
        conn.close()


@app.route('/get_ad_entities')
def get_ad_entities():
    if not session.get('logged_in'): return jsonify([])
    q = request.args.get('q', '').strip()
    if len(q) < 2: return jsonify([])
    try:
        server = Server(LDAP_CONFIG['uri'], get_info=ALL)
        conn = Connection(server, user=LDAP_CONFIG['bind_dn'], password=LDAP_CONFIG['bind_password'], auto_bind=True)
        search_filter = f"(|(sAMAccountName=*{q}*)(displayName=*{q}*)(cn=*{q}*))"
        conn.search(LDAP_CONFIG['base'], search_filter, SUBTREE,
                    attributes=['sAMAccountName', 'displayName', 'objectClass', 'cn'])
        results = []
        for entry in conn.entries:
            is_group = 'group' in entry.objectClass
            login_val = str(entry.cn) if is_group else str(entry.sAMAccountName)
            results.append({'login': login_val, 'name': str(entry.displayName) if entry.displayName else str(entry.cn),
                            'type': 'Группа AD' if is_group else 'Юзер'})
        return jsonify(results[:20])
    except:
        return jsonify([])


@app.route('/get_resources')
def get_resources():
    if not session.get('logged_in'): return jsonify([]), 403
    u = session.get('username')
    search_query = request.args.get('search', '').strip().lower()
    user_ad_groups = session.get('ad_groups', [])
    conn = get_db_connection()
    try:
        rows = conn.execute('''
            SELECT r.*, GROUP_CONCAT(ga.group_id) as group_ids
            FROM resources r
            LEFT JOIN resource_group_access ga ON r.id = ga.resource_id
            GROUP BY r.id
            ORDER BY r.position ASC
        ''').fetchall()
        def matches_search(resource_row):
            if not search_query:
                return True
            haystack = " ".join([
                str(resource_row.get('title') or ''),
                str(resource_row.get('desc') or ''),
                str(resource_row.get('category') or ''),
                str(resource_row.get('url') or '')
            ]).lower()
            return search_query in haystack

        if u in MASTER_ADMINS:
            admin_rows = [dict(row) for row in rows]
            return jsonify([row for row in admin_rows if matches_search(row)])

        members_rows = conn.execute("SELECT group_id, username FROM group_members").fetchall()
        group_map = {}
        for mr in members_rows:
            gid = str(mr['group_id'])
            if gid not in group_map: group_map[gid] = []
            group_map[gid].append(mr['username'].lower())

        visible = []
        for r in rows:
            g_ids = r['group_ids'].split(',') if r['group_ids'] else []
            row_dict = dict(r)
            if not g_ids:
                if matches_search(row_dict):
                    visible.append(row_dict)
                continue
            can_see = False
            for gid in g_ids:
                allowed_entities = group_map.get(gid, [])
                if u in allowed_entities or any(ag in allowed_entities for ag in user_ad_groups):
                    can_see = True
                    break
            if can_see and matches_search(row_dict):
                visible.append(row_dict)
        return jsonify(visible)
    finally:
        conn.close()


@app.route('/add', methods=['POST'])
def add_resource():
    if session.get('username') not in MASTER_ADMINS: return jsonify(success=False), 403
    t, u = request.form.get('title'), request.form.get('url')
    c_existing = (request.form.get('category_existing') or '').strip()
    if c_existing == '__new__':
        c_existing = ''
    c_new = (request.form.get('category_new') or '').strip()
    c = c_new or c_existing or (request.form.get('category') or '').strip()
    d = request.form.get('desc')
    if not c:
        return jsonify(success=False, error='Укажите раздел'), 400
    gids = request.form.getlist('access_group_ids')
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        cur.execute("INSERT INTO resources (title, url, category, desc) VALUES (?, ?, ?, ?)", (t, u, c, d))
        cur.execute("INSERT OR IGNORE INTO categories (name) VALUES (?)", (c,))
        rid = cur.lastrowid
        for gid in gids: cur.execute("INSERT INTO resource_group_access (resource_id, group_id) VALUES (?, ?)",
                                     (rid, int(gid)))
        conn.commit()
        return jsonify(success=True)
    finally:
        conn.close()


@app.route('/edit/<int:res_id>', methods=['POST'])
def edit_resource(res_id):
    if session.get('username') not in MASTER_ADMINS: return jsonify(success=False), 403
    t, u = request.form.get('title'), request.form.get('url')
    c_existing = (request.form.get('category_existing') or '').strip()
    if c_existing == '__new__':
        c_existing = ''
    c_new = (request.form.get('category_new') or '').strip()
    c = c_new or c_existing or (request.form.get('category') or '').strip()
    d = request.form.get('desc')
    if not c:
        return jsonify(success=False, error='Укажите раздел'), 400
    gids = request.form.getlist('access_group_ids')
    conn = get_db_connection()
    try:
        conn.execute("UPDATE resources SET title=?, url=?, category=?, desc=? WHERE id=?", (t, u, c, d, res_id))
        conn.execute("INSERT OR IGNORE INTO categories (name) VALUES (?)", (c,))
        conn.execute("DELETE FROM resource_group_access WHERE resource_id=?", (res_id,))
        for gid in gids: conn.execute("INSERT INTO resource_group_access (resource_id, group_id) VALUES (?, ?)",
                                      (res_id, int(gid)))
        conn.commit()
        return jsonify(success=True)
    finally:
        conn.close()


@app.route('/delete/<int:res_id>', methods=['POST'])
def delete_resource(res_id):
    if session.get('username') not in MASTER_ADMINS: return jsonify(success=False), 403
    conn = get_db_connection()
    try:
        conn.execute("DELETE FROM resources WHERE id=?", (res_id,))
        conn.execute("DELETE FROM resource_group_access WHERE resource_id=?", (res_id,))
        conn.commit()
        return jsonify(success=True)
    finally:
        conn.close()


@app.route('/reorder', methods=['POST'])
def reorder():
    if session.get('username') not in MASTER_ADMINS: return jsonify(success=False), 403
    conn = get_db_connection()
    try:
        for index, entry in enumerate(request.json):
            conn.execute("UPDATE resources SET position=?, category=? WHERE id=?",
                         (index, entry['category'], entry['id']))
        conn.commit()
        return jsonify(success=True)
    finally:
        conn.close()


@app.route('/manage_category', methods=['POST'])
def manage_category():
    if session.get('username') not in MASTER_ADMINS:
        return jsonify(success=False), 403
    data = request.json or {}
    action = (data.get('action') or '').strip()
    conn = get_db_connection()
    try:
        if action == 'create':
            category_name = (data.get('category_name') or '').strip()
            if not category_name:
                return jsonify(success=False, error='Укажите название нового раздела'), 400
            exists = conn.execute('SELECT 1 FROM categories WHERE name = ?', (category_name,)).fetchone()
            if exists:
                return jsonify(success=False, error='Раздел с таким названием уже существует'), 409
            conn.execute('INSERT INTO categories (name) VALUES (?)', (category_name,))
            conn.commit()
            return jsonify(success=True)

        if action == 'rename':
            old_name = (data.get('old_name') or '').strip()
            new_name = (data.get('new_name') or '').strip()
            if not old_name or not new_name:
                return jsonify(success=False, error='Укажите старое и новое название раздела'), 400
            if old_name == new_name:
                return jsonify(success=False, error='Название раздела не изменилось'), 400
            exists = conn.execute('SELECT 1 FROM categories WHERE name = ?', (old_name,)).fetchone()
            if not exists:
                return jsonify(success=False, error='Раздел не найден'), 404
            conflict = conn.execute('SELECT 1 FROM categories WHERE name = ?', (new_name,)).fetchone()
            if conflict:
                return jsonify(success=False, error='Раздел с таким названием уже существует'), 409
            conn.execute('UPDATE resources SET category = ? WHERE category = ?', (new_name, old_name))
            conn.execute('INSERT OR IGNORE INTO categories (name) VALUES (?)', (new_name,))
            conn.execute('DELETE FROM categories WHERE name = ?', (old_name,))
            conn.commit()
            return jsonify(success=True)

        if action == 'delete':
            category_name = (data.get('category_name') or '').strip()
            transfer_mode = (data.get('transfer_mode') or '').strip()
            target_category = (data.get('target_category') or '').strip()
            resource_moves = data.get('resource_moves') or {}
            if not category_name:
                return jsonify(success=False, error='Укажите раздел для удаления'), 400
            exists = conn.execute('SELECT 1 FROM categories WHERE name = ?', (category_name,)).fetchone()
            if not exists:
                return jsonify(success=False, error='Раздел не найден'), 404
            resources = conn.execute(
                'SELECT id FROM resources WHERE category = ? ORDER BY id',
                (category_name,)
            ).fetchall()
            resource_ids = [str(r['id']) for r in resources]

            if resource_ids:
                if transfer_mode == 'single':
                    if not target_category:
                        return jsonify(success=False, error='Выберите целевой раздел для ресурсов'), 400
                    if target_category == category_name:
                        return jsonify(success=False, error='Нельзя переносить в удаляемый раздел'), 400
                    conn.execute('UPDATE resources SET category = ? WHERE category = ?', (target_category, category_name))
                    conn.execute('INSERT OR IGNORE INTO categories (name) VALUES (?)', (target_category,))
                elif transfer_mode == 'split':
                    if not isinstance(resource_moves, dict):
                        return jsonify(success=False, error='Некорректные данные распределения'), 400
                    targets_to_create = set()
                    for rid in resource_ids:
                        target = str(resource_moves.get(rid, '')).strip()
                        if not target:
                            return jsonify(success=False, error='Укажите целевой раздел для каждого ресурса'), 400
                        if target == category_name:
                            return jsonify(success=False, error='Нельзя переносить в удаляемый раздел'), 400
                        targets_to_create.add(target)
                    for t in targets_to_create:
                        conn.execute('INSERT OR IGNORE INTO categories (name) VALUES (?)', (t,))
                    for rid in resource_ids:
                        target = str(resource_moves.get(rid, '')).strip()
                        conn.execute('UPDATE resources SET category = ? WHERE id = ?', (target, int(rid)))
                elif transfer_mode == 'delete_all':
                    conn.execute(
                        'DELETE FROM resource_group_access WHERE resource_id IN (SELECT id FROM resources WHERE category = ?)',
                        (category_name,)
                    )
                    conn.execute('DELETE FROM resources WHERE category = ?', (category_name,))
                else:
                    return jsonify(success=False, error='Выберите режим обработки ресурсов'), 400

            conn.execute('DELETE FROM categories WHERE name = ?', (category_name,))
            conn.commit()
            return jsonify(success=True)

        return jsonify(success=False, error='Неизвестное действие'), 400
    finally:
        conn.close()


@app.route('/manage_group', methods=['POST'])
def manage_group():
    if session.get('username') not in MASTER_ADMINS: return jsonify(success=False), 403
    data = request.json
    action, name, gid = data.get('action'), data.get('name'), data.get('id')
    conn = get_db_connection()
    try:
        if action == 'add':
            conn.execute('INSERT OR IGNORE INTO groups (name) VALUES (?)', (name,))
        elif action == 'delete':
            conn.execute('DELETE FROM groups WHERE id = ?', (gid,))
            conn.execute('DELETE FROM group_members WHERE group_id = ?', (gid,))
            conn.execute('DELETE FROM resource_group_access WHERE group_id = ?', (gid,))
        elif action == 'update_members':
            conn.execute('DELETE FROM group_members WHERE group_id = ?', (gid,))
            for m in data.get('members', []):
                if m.strip(): conn.execute('INSERT INTO group_members (group_id, username) VALUES (?, ?)',
                                           (gid, m.strip().lower()))
        conn.commit()
        return jsonify(success=True)
    finally:
        conn.close()


if __name__ == '__main__':
    init_db()
    app.run(host='0.0.0.0', port=5004, debug=True, threaded=True)