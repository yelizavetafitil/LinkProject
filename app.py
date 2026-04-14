import sqlite3
import os
import collections
from flask import Flask, render_template, request, session, jsonify, redirect, url_for, send_from_directory

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


def get_db_connection():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.execute('PRAGMA journal_mode=WAL;')
    conn.row_factory = sqlite3.Row
    return conn


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
        # Синхронизируем справочник разделов с уже существующими ресурсами
        existing_categories = conn.execute(
            "SELECT DISTINCT TRIM(category) AS category FROM resources WHERE TRIM(category) <> ''"
        ).fetchall()
        for row in existing_categories:
            conn.execute('INSERT OR IGNORE INTO categories (name) VALUES (?)', (row['category'],))
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


@app.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('login_page'))


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