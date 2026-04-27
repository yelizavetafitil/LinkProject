import sqlite3
import os
import collections
import re
import json
import time
import threading
import calendar
import smtplib
from email.message import EmailMessage
from email.utils import format_datetime
from email.header import Header
from email import policy
from urllib.parse import urlparse
from datetime import datetime
from datetime import timedelta
from zoneinfo import ZoneInfo
from flask import Flask, render_template, request, session, jsonify, redirect, url_for, send_from_directory, send_file
import pandas as pd
import openpyxl
try:
    import xlrd
except Exception:
    xlrd = None

# --- ЗАПЛАТКА ДЛЯ LDAP3 ---
if not hasattr(collections, 'MutableMapping'):
    import collections.abc

    collections.MutableMapping = collections.abc.MutableMapping
if not hasattr(collections, 'Sequence'):
    import collections.abc

    collections.Sequence = collections.abc.Sequence

from ldap3 import Server, Connection, ALL, SUBTREE
from ldap3.core.exceptions import LDAPException

app = Flask(__name__)
app.secret_key = 'enterprise_hub_production_v37'
app.config.update(
    SESSION_COOKIE_NAME='enterprise_hub_session',
    SESSION_COOKIE_PATH='/',
    SESSION_COOKIE_HTTPONLY=True,
    SESSION_COOKIE_SAMESITE='Lax',
    PERMANENT_SESSION_LIFETIME=timedelta(hours=12),
    SESSION_REFRESH_EACH_REQUEST=False
)

LDAP_CONFIG = {
    'uri': "ldap://192.168.0.4",
    'base': "DC=local,DC=energoprom,DC=by",
    'bind_dn': "CN=OC1,OU=ОЦ,DC=local,DC=energoprom,DC=by",
    'bind_password': "Pass5678",
    'user_attr': "sAMAccountName"
}

MASTER_ADMINS = ['rapeiko', 'oc1']
DB_PATH = 'database.db'
GYM_ROOM_NAME = 'Спортзал'
LOGO_FILENAME = 'image2_hq.png'
PHONEBOOK_PATH = 'phonebook.xlsx'
PHONEBOOK_COLUMNS = ['dept', 'pos', 'surname', 'name', 'work', 'home', 'mobile']
MAIL_DOMAIN = 'energoprom.by'
MAIL_SENDER = f'robot-bnp@{MAIL_DOMAIN}'
SMTP_HOST = os.environ.get('SMTP_HOST', '192.168.0.28')
SMTP_PORT = int(os.environ.get('SMTP_PORT', '587'))
SMTP_USERNAME = os.environ.get('SMTP_USERNAME', 'robot-bnp')
SMTP_PASSWORD = os.environ.get('SMTP_PASSWORD', '^nO@u(Flu5+zH&v>')
SMTP_USE_TLS = os.environ.get('SMTP_USE_TLS', '1') == '1'
APP_TZ = ZoneInfo('Europe/Minsk')
_phonebook_cache = None
_phonebook_mtime = None
_ad_groups_cache = {}
TABEL_BASE_DIR = os.environ.get('TABEL_BASE_DIR', r'\\srv-doc\ТАБЕЛЬ')
TABEL_LEADERS_FILE = os.environ.get('TABEL_LEADERS_FILE', r'\\srv-doc\ТАБЕЛЬ\ОЦ\Список руководителей.xlsx')
TABEL_SHEET_NAME = os.environ.get('TABEL_SHEET_NAME', 'Табель_3')
TABEL_CACHE_FILE = os.environ.get('TABEL_CACHE_FILE', 'tabel_portal_cache.json')
TABEL_FILE_PATTERN = re.compile(r'^(.+?)_(\d{2})_(\d{2})\.xlsx?$', re.IGNORECASE)
TABEL_FIO_RE = re.compile(r'^[А-ЯЁ][а-яё\-]+\s+[А-ЯЁ]\s*\.\s*[А-ЯЁ]\s*\.\s*$', re.IGNORECASE)
TABEL_STATUS_MAP = {
    'Б': 'Больничный', 'Н': 'Неявки', 'Р': 'Роды', 'ОЖ': 'Уход за ребенком',
    'О': 'Отпуск', 'ЛО': 'Социальный отпуск', 'А': 'Отпуск без сохранения', 'Т': 'Нетрудоспособность',
    'Х': 'Уход за больным', 'ПР': 'Прогул', 'Г': 'Гособязанность', 'ОА': 'Инициатива нанимателя',
    'Д': 'Донор', 'У': 'Учеба', 'УД': 'Учебные дни', 'ДМ': 'День матери',
    'ДСП': 'Диспансеризация', 'П': 'Праздник', 'К': 'Командировка', 'В': 'Выходной'
}
TABEL_INDEX_LOCK = threading.Lock()
TABEL_INDEX = {}
TABEL_FILE_CACHE = {}
TABEL_LEADERS_CACHE = {}
TABEL_LEADERS_MTIME = 0.0
TABEL_LAST_SCAN_TS = 0.0
TABEL_SCAN_INTERVAL_SEC = 180
KNOWLEDGE_BASE_ROOT = os.environ.get('KNOWLEDGE_BASE_ROOT', os.path.join(app.root_path, 'БАЗА ЗНАНИЙ'))
KNOWLEDGE_BASE_INSTRUCTIONS_DIR = os.environ.get(
    'KNOWLEDGE_BASE_INSTRUCTIONS_DIR',
    KNOWLEDGE_BASE_ROOT
)


def normalize_resource_url(raw_url):
    value = (raw_url or '').strip()
    if not value:
        return value
    try:
        parsed = urlparse(value)
    except Exception:
        return value
    host = (parsed.hostname or '').lower()
    if host in ('127.0.0.1', 'localhost', '::1'):
        path = parsed.path or '/'
        query = f'?{parsed.query}' if parsed.query else ''
        fragment = f'#{parsed.fragment}' if parsed.fragment else ''
        return f'{path}{query}{fragment}'
    return value


def normalize_ad_username(raw_username):
    username = (raw_username or '').strip().lower()
    if not username:
        return ''
    if '\\' in username:
        username = username.split('\\')[-1].strip()
    if '@' in username:
        username = username.split('@')[0].strip()
    return username


def _tabel_clean_fio(raw_val):
    if pd.isna(raw_val):
        return ''
    text = str(raw_val).strip()
    return text.replace('\xa0', ' ').replace('\u200b', '')


def _tabel_is_work_value(value_str):
    if value_str in ('', '0', '0.0'):
        return False
    return value_str.replace('.', '', 1).replace(',', '', 1).isdigit()


def _tabel_parse_filename(filename):
    match = TABEL_FILE_PATTERN.match(filename or '')
    if not match:
        return None, None
    return match.group(2), match.group(3)


def _tabel_read_any_excel(path):
    if not path or not os.path.exists(path):
        return None
    try:
        if path.lower().endswith('.xlsx'):
            return pd.read_excel(path, sheet_name=TABEL_SHEET_NAME, engine='openpyxl', header=None)
        if xlrd is None:
            return None
        workbook = xlrd.open_workbook(path, formatting_info=False)
        sheet = workbook.sheet_by_name(TABEL_SHEET_NAME)
        return pd.DataFrame([sheet.row_values(i) for i in range(sheet.nrows)])
    except Exception:
        return None


def _tabel_load_cache():
    global TABEL_FILE_CACHE
    if not os.path.exists(TABEL_CACHE_FILE):
        TABEL_FILE_CACHE = {}
        return
    try:
        with open(TABEL_CACHE_FILE, 'r', encoding='utf-8') as cache_file:
            parsed = json.load(cache_file)
            TABEL_FILE_CACHE = parsed if isinstance(parsed, dict) else {}
    except Exception:
        TABEL_FILE_CACHE = {}


def _tabel_save_cache():
    try:
        with open(TABEL_CACHE_FILE, 'w', encoding='utf-8') as cache_file:
            json.dump(TABEL_FILE_CACHE, cache_file, ensure_ascii=False)
    except Exception:
        return


def _tabel_rebuild_index_from_cache():
    local_index = {}
    for file_path, data in TABEL_FILE_CACHE.items():
        if not isinstance(data, dict):
            continue
        dept = data.get('dept')
        yy_mm = data.get('yy_mm')
        emps = data.get('emps')
        if not dept or not yy_mm or not isinstance(emps, list):
            continue
        local_index.setdefault(dept, {}).setdefault(yy_mm, []).append({'file': file_path, 'employees': emps})
    with TABEL_INDEX_LOCK:
        TABEL_INDEX.clear()
        TABEL_INDEX.update(local_index)


def _scan_tabel_base_dir():
    global TABEL_FILE_CACHE
    current_year = datetime.now().year
    found_files = set()
    cache_updated = False
    for root, _, files in os.walk(TABEL_BASE_DIR):
        rel = os.path.relpath(root, TABEL_BASE_DIR)
        dept = rel.split(os.sep)[0] if rel != '.' else 'Общий'
        for filename in files:
            if not filename.lower().endswith(('.xls', '.xlsx')) or filename.startswith('~$'):
                continue
            mm, yy = _tabel_parse_filename(filename)
            if not mm:
                continue
            try:
                if 2000 + int(yy) < current_year - 1:
                    continue
            except Exception:
                continue
            full_path = os.path.join(root, filename)
            found_files.add(full_path)
            try:
                current_mtime = os.path.getmtime(full_path)
            except OSError:
                continue
            cached = TABEL_FILE_CACHE.get(full_path)
            if isinstance(cached, dict) and cached.get('mtime') == current_mtime:
                continue
            df = _tabel_read_any_excel(full_path)
            if df is None:
                continue
            employees = []
            try:
                for row_index, raw_name in enumerate(df.iloc[:, 1]):
                    name = _tabel_clean_fio(raw_name)
                    if not name or not TABEL_FIO_RE.match(name):
                        continue
                    row_data = df.iloc[row_index].values
                    days_data = []
                    for day_idx in range(31):
                        col_idx = 2 + day_idx
                        if col_idx < len(row_data):
                            day_val = row_data[col_idx]
                            days_data.append(str(day_val).strip() if not pd.isna(day_val) else '')
                        else:
                            days_data.append('')
                    employees.append({'fio': name, 'days': days_data})
            except Exception:
                continue
            if employees:
                TABEL_FILE_CACHE[full_path] = {
                    'mtime': current_mtime,
                    'dept': dept,
                    'yy_mm': f'{yy}_{mm}',
                    'emps': employees
                }
                cache_updated = True
    deleted_files = set(TABEL_FILE_CACHE.keys()) - found_files
    if deleted_files:
        for deleted_path in deleted_files:
            TABEL_FILE_CACHE.pop(deleted_path, None)
        cache_updated = True
    if cache_updated:
        _tabel_save_cache()
    _tabel_rebuild_index_from_cache()


def ensure_tabel_index(force=False):
    global TABEL_LAST_SCAN_TS
    now_ts = time.time()
    if not force and (now_ts - TABEL_LAST_SCAN_TS) < TABEL_SCAN_INTERVAL_SEC and TABEL_INDEX:
        return
    with TABEL_INDEX_LOCK:
        # Повторно проверяем внутри lock, чтобы не запускать конкурентные сканы.
        if not force and (time.time() - TABEL_LAST_SCAN_TS) < TABEL_SCAN_INTERVAL_SEC and TABEL_INDEX:
            return
    _scan_tabel_base_dir()
    TABEL_LAST_SCAN_TS = time.time()


def _tabel_get_current_status(fio):
    now = datetime.now()
    curr_period = f'{str(now.year)[2:]}_{now.month:02d}'
    day_idx = now.day - 1
    with TABEL_INDEX_LOCK:
        for dep_data in TABEL_INDEX.values():
            records = dep_data.get(curr_period) or []
            for rec in records:
                match = next((item for item in rec.get('employees', []) if item.get('fio') == fio), None)
                if not match:
                    continue
                days = match.get('days') or []
                if day_idx >= len(days):
                    return 'unknown'
                val = str(days[day_idx]).strip().upper()
                if val == '':
                    return 'absent'
                if _tabel_is_work_value(val):
                    return 'work'
                if val == 'В':
                    return 'rest'
                return 'absent'
    return 'unknown'


def get_tabel_leaders_data():
    global TABEL_LEADERS_CACHE, TABEL_LEADERS_MTIME
    categories = [
        'Руководство',
        'Персонал при руководстве',
        'Производственные отделы',
        'Главные инженеры проекта',
        'Непроизводственные отделы'
    ]
    try:
        current_mtime = os.path.getmtime(TABEL_LEADERS_FILE) if os.path.exists(TABEL_LEADERS_FILE) else 0
    except Exception:
        current_mtime = 0
    if TABEL_LEADERS_CACHE and current_mtime == TABEL_LEADERS_MTIME:
        for category in categories:
            for leader in TABEL_LEADERS_CACHE.get(category, []):
                leader['status_cls'] = f"st-{_tabel_get_current_status(leader.get('fio', ''))}"
        return TABEL_LEADERS_CACHE
    data = {category: [] for category in categories}
    if not os.path.exists(TABEL_LEADERS_FILE):
        return data
    try:
        workbook = openpyxl.load_workbook(TABEL_LEADERS_FILE, data_only=True)
        sheet = workbook.active
        current_category = None
        for row in sheet.iter_rows(values_only=True):
            first_val = _tabel_clean_fio(row[0] if row else '')
            if not first_val:
                continue
            matched_category = next((cat for cat in categories if cat.lower() == first_val.lower()), None)
            if matched_category:
                current_category = matched_category
                continue
            if current_category and TABEL_FIO_RE.match(first_val):
                status = _tabel_get_current_status(first_val)
                data[current_category].append({
                    'fio': first_val,
                    'info': f"{str((row[1] if len(row) > 1 else '') or '')} {str((row[2] if len(row) > 2 else '') or '')}".strip(),
                    'status_cls': f'st-{status}'
                })
        TABEL_LEADERS_CACHE = data
        TABEL_LEADERS_MTIME = current_mtime
    except Exception:
        return data
    return data


def get_db_connection():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.execute('PRAGMA journal_mode=WAL;')
    conn.row_factory = sqlite3.Row
    return conn


def ensure_gym_room_exists():
    with get_db_connection() as conn:
        conn.execute('INSERT OR IGNORE INTO meeting_rooms (name) VALUES (?)', (GYM_ROOM_NAME,))
        conn.commit()


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


def can_view_extended_phonebook(username):
    login = normalize_ad_username(username)
    if not login:
        return False
    if login in MASTER_ADMINS:
        return True
    return _has_privileged_entity_access(login, 'phonebook_privileged_entities')


def _has_privileged_entity_access(login, table_name):
    user_groups = {group.strip().lower() for group in get_user_ad_groups_by_username(login)}
    conn = get_db_connection()
    try:
        entity_rows = conn.execute(
            f'SELECT entity_type, entity_login FROM {table_name}'
        ).fetchall()
    finally:
        conn.close()
    if not entity_rows:
        return False
    allowed_users = set()
    allowed_groups = set()
    for row in entity_rows:
        entity_type = (row['entity_type'] or '').strip().lower()
        entity_login = (row['entity_login'] or '').strip().lower()
        if not entity_login:
            continue
        if entity_type == 'group':
            allowed_groups.add(entity_login)
        else:
            allowed_users.add(entity_login)
    if login in allowed_users:
        return True
    if user_groups.intersection(allowed_groups):
        return True
    return False


def can_manage_all_bookings(username):
    login = normalize_ad_username(username)
    if not login:
        return False
    if login in MASTER_ADMINS:
        return True
    return _has_privileged_entity_access(login, 'booking_privileged_entities')


def can_manage_resources(username):
    login = normalize_ad_username(username)
    if not login:
        return False
    if login in MASTER_ADMINS:
        return True
    return _has_privileged_entity_access(login, 'resource_privileged_entities')


def _knowledge_base_collect_categories():
    categories = []
    base_path = os.path.realpath(KNOWLEDGE_BASE_INSTRUCTIONS_DIR)
    if not os.path.isdir(base_path):
        return categories
    for name in os.listdir(base_path):
        full_path = os.path.join(base_path, name)
        if not os.path.isdir(full_path):
            continue
        categories.append(name)
    return sorted(categories, key=lambda item: item.lower())


def _knowledge_base_resolve_category_path(category_name):
    base_path = os.path.realpath(KNOWLEDGE_BASE_INSTRUCTIONS_DIR)
    category_path = os.path.realpath(os.path.join(base_path, category_name))
    if not category_path.startswith(base_path + os.sep):
        return None
    if not os.path.isdir(category_path):
        return None
    return category_path


def _knowledge_base_collect_files(category_name):
    category_path = _knowledge_base_resolve_category_path(category_name)
    if not category_path:
        return []
    file_items = []
    for root, _, files in os.walk(category_path):
        for filename in files:
            if not filename.lower().endswith('.pdf'):
                continue
            full_path = os.path.join(root, filename)
            rel_path = os.path.relpath(full_path, category_path).replace('\\', '/')
            file_items.append({
                'name': filename,
                'path': rel_path
            })
    return sorted(file_items, key=lambda item: item['path'].lower())


def _knowledge_base_collect_all_files():
    items = []
    for category in _knowledge_base_collect_categories():
        for file_item in _knowledge_base_collect_files(category):
            items.append({
                'category': category,
                'name': file_item.get('name', ''),
                'path': file_item.get('path', '')
            })
    return items


def _knowledge_base_resolve_file_path(category_name, relative_file_path):
    category_path = _knowledge_base_resolve_category_path(category_name)
    if not category_path:
        return None
    normalized_rel = (relative_file_path or '').replace('\\', '/').strip('/')
    if not normalized_rel or not normalized_rel.lower().endswith('.pdf'):
        return None
    target_path = os.path.realpath(os.path.join(category_path, normalized_rel))
    if not target_path.startswith(category_path + os.sep):
        return None
    if not os.path.isfile(target_path):
        return None
    return target_path


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
                participants_json TEXT DEFAULT '[]',
                meeting_date TEXT NOT NULL,
                start_time TEXT NOT NULL,
                end_time TEXT NOT NULL,
                booking_status TEXT DEFAULT 'active',
                canceled_by TEXT,
                canceled_at TEXT,
                owner_username TEXT NOT NULL,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(room_id) REFERENCES meeting_rooms(id)
            )''')
        conn.execute('''
            CREATE TABLE IF NOT EXISTS meeting_booking_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                booking_id INTEGER NOT NULL,
                action TEXT NOT NULL,
                changed_by TEXT NOT NULL,
                changed_at TEXT NOT NULL,
                details_json TEXT NOT NULL,
                FOREIGN KEY(booking_id) REFERENCES meeting_bookings(id)
            )''')
        conn.execute('''
            CREATE TABLE IF NOT EXISTS driver_trips (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                vehicle_model TEXT NOT NULL,
                vehicle_color TEXT NOT NULL DEFAULT '#1f77b4',
                trip_date TEXT NOT NULL,
                departure_time TEXT NOT NULL,
                origin TEXT NOT NULL DEFAULT 'РУП «Белнипиэнергопром»',
                destination TEXT NOT NULL,
                description TEXT,
                trip_status TEXT NOT NULL DEFAULT 'active',
                canceled_by TEXT,
                canceled_at TEXT,
                owner_username TEXT,
                created_by TEXT NOT NULL,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP
            )''')
        conn.execute('''
            CREATE TABLE IF NOT EXISTS driver_trip_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                trip_id INTEGER NOT NULL,
                action TEXT NOT NULL,
                changed_by TEXT NOT NULL,
                changed_at TEXT NOT NULL,
                details_json TEXT NOT NULL,
                FOREIGN KEY(trip_id) REFERENCES driver_trips(id)
            )''')
        conn.execute('''
            CREATE TABLE IF NOT EXISTS phonebook_privileged_users (
                username TEXT PRIMARY KEY
            )''')
        conn.execute('''
            CREATE TABLE IF NOT EXISTS phonebook_privileged_entities (
                entity_type TEXT NOT NULL,
                entity_login TEXT NOT NULL,
                PRIMARY KEY (entity_type, entity_login)
            )''')
        conn.execute('''
            CREATE TABLE IF NOT EXISTS booking_privileged_entities (
                entity_type TEXT NOT NULL,
                entity_login TEXT NOT NULL,
                PRIMARY KEY (entity_type, entity_login)
            )''')
        conn.execute('''
            CREATE TABLE IF NOT EXISTS resource_privileged_entities (
                entity_type TEXT NOT NULL,
                entity_login TEXT NOT NULL,
                PRIMARY KEY (entity_type, entity_login)
            )''')
        # Миграция старого формата (только пользователи) в новый универсальный справочник.
        legacy_rows = conn.execute('SELECT username FROM phonebook_privileged_users').fetchall()
        for row in legacy_rows:
            legacy_login = normalize_ad_username(row['username'])
            if not legacy_login:
                continue
            conn.execute(
                'INSERT OR IGNORE INTO phonebook_privileged_entities (entity_type, entity_login) VALUES (?, ?)',
                ('user', legacy_login)
            )
        booking_columns = conn.execute("PRAGMA table_info(meeting_bookings)").fetchall()
        booking_column_names = {row['name'] for row in booking_columns}
        if 'participants_json' not in booking_column_names:
            conn.execute("ALTER TABLE meeting_bookings ADD COLUMN participants_json TEXT DEFAULT '[]'")
        if 'booking_status' not in booking_column_names:
            conn.execute("ALTER TABLE meeting_bookings ADD COLUMN booking_status TEXT DEFAULT 'active'")
            conn.execute("UPDATE meeting_bookings SET booking_status = 'active' WHERE booking_status IS NULL")
        if 'canceled_by' not in booking_column_names:
            conn.execute("ALTER TABLE meeting_bookings ADD COLUMN canceled_by TEXT")
        if 'canceled_at' not in booking_column_names:
            conn.execute("ALTER TABLE meeting_bookings ADD COLUMN canceled_at TEXT")
        driver_trip_columns = conn.execute("PRAGMA table_info(driver_trips)").fetchall()
        driver_trip_column_names = {row['name'] for row in driver_trip_columns}
        if 'vehicle_color' not in driver_trip_column_names:
            conn.execute("ALTER TABLE driver_trips ADD COLUMN vehicle_color TEXT NOT NULL DEFAULT '#1f77b4'")
        if 'trip_status' not in driver_trip_column_names:
            conn.execute("ALTER TABLE driver_trips ADD COLUMN trip_status TEXT NOT NULL DEFAULT 'active'")
            conn.execute("UPDATE driver_trips SET trip_status = 'active' WHERE trip_status IS NULL")
        if 'canceled_by' not in driver_trip_column_names:
            conn.execute("ALTER TABLE driver_trips ADD COLUMN canceled_by TEXT")
        if 'canceled_at' not in driver_trip_column_names:
            conn.execute("ALTER TABLE driver_trips ADD COLUMN canceled_at TEXT")
        if 'owner_username' not in driver_trip_column_names:
            conn.execute("ALTER TABLE driver_trips ADD COLUMN owner_username TEXT")
            conn.execute("UPDATE driver_trips SET owner_username = created_by WHERE owner_username IS NULL OR owner_username = ''")
        if 'updated_at' not in driver_trip_column_names:
            conn.execute("ALTER TABLE driver_trips ADD COLUMN updated_at TEXT")
            conn.execute("UPDATE driver_trips SET updated_at = CURRENT_TIMESTAMP WHERE updated_at IS NULL")
        # Синхронизируем справочник разделов с уже существующими ресурсами
        existing_categories = conn.execute(
            "SELECT DISTINCT TRIM(category) AS category FROM resources WHERE TRIM(category) <> ''"
        ).fetchall()
        for row in existing_categories:
            conn.execute('INSERT OR IGNORE INTO categories (name) VALUES (?)', (row['category'],))
        # Нормализуем внутренние URL ресурсов, чтобы проект корректно открывался на любом ПК.
        resource_rows = conn.execute('SELECT id, url FROM resources').fetchall()
        for row in resource_rows:
            normalized_url = normalize_resource_url(row['url'])
            if normalized_url != row['url']:
                conn.execute('UPDATE resources SET url = ? WHERE id = ?', (normalized_url, row['id']))
        default_rooms = ['Переговорка 1', 'Переговорка 2', 'Конференц-зал', GYM_ROOM_NAME]
        for room_name in default_rooms:
            conn.execute('INSERT OR IGNORE INTO meeting_rooms (name) VALUES (?)', (room_name,))
        driver_resource_exists = conn.execute(
            'SELECT 1 FROM resources WHERE TRIM(url) = ? LIMIT 1',
            ('/driver-trips',)
        ).fetchone()
        if not driver_resource_exists:
            conn.execute(
                'INSERT INTO resources (title, url, category, desc, position) VALUES (?, ?, ?, ?, ?)',
                (
                    'Сервис водителей',
                    '/driver-trips',
                    'Сервисы',
                    'Рейсы и командировки водителей за пределы г. Минска',
                    0
                )
            )
            conn.execute('INSERT OR IGNORE INTO categories (name) VALUES (?)', ('Сервисы',))
        conn.commit()
def get_user_ad_groups(conn, user_dn):
    search_filter = f"(member:1.2.840.113556.1.4.1941:={user_dn})"
    conn.search(LDAP_CONFIG['base'], search_filter, SUBTREE, attributes=['sAMAccountName', 'cn'])
    groups = []
    for entry in conn.entries:
        if entry.sAMAccountName: groups.append(str(entry.sAMAccountName).lower())
        if entry.cn: groups.append(str(entry.cn).lower())
    return list(set(groups))


def get_user_ad_groups_by_username(username):
    login = normalize_ad_username(username)
    if not login:
        return []
    cached = _ad_groups_cache.get(login)
    if isinstance(cached, list):
        return cached
    try:
        server = Server(LDAP_CONFIG['uri'], get_info=ALL, connect_timeout=5)
        conn = Connection(server, user=LDAP_CONFIG['bind_dn'], password=LDAP_CONFIG['bind_password'], auto_bind=True)
        search_filter = f"({LDAP_CONFIG['user_attr']}={login})"
        conn.search(LDAP_CONFIG['base'], search_filter, SUBTREE, attributes=['distinguishedName'])
        if not conn.entries:
            _ad_groups_cache[login] = []
            return []
        user_dn = conn.entries[0].distinguishedName.value
        groups = get_user_ad_groups(conn, user_dn)
        _ad_groups_cache[login] = groups
        return groups
    except Exception:
        return []


def check_ldap_auth(username, password):
    try:
        server = Server(LDAP_CONFIG['uri'], get_info=ALL, connect_timeout=5)
        conn = Connection(server, user=LDAP_CONFIG['bind_dn'], password=LDAP_CONFIG['bind_password'], auto_bind=True)
        search_filter = f"({LDAP_CONFIG['user_attr']}={username})"
        conn.search(LDAP_CONFIG['base'], search_filter, SUBTREE, attributes=['distinguishedName', 'displayName', 'cn'])
        if not conn.entries:
            return False, 'Пользователь не найден в AD'
        user_entry = conn.entries[0]
        user_dn = user_entry.distinguishedName.value
        display_name = ''
        if user_entry.displayName:
            display_name = str(user_entry.displayName).strip()
        if not display_name and user_entry.cn:
            display_name = str(user_entry.cn).strip()
        Connection(server, user=user_dn, password=password, auto_bind=True)
        session['display_name'] = display_name or username
        _ad_groups_cache[normalize_ad_username(username)] = get_user_ad_groups(conn, user_dn)
        session.permanent = True
        return True, ''
    except LDAPException as exc:
        app.logger.warning('LDAP auth failed for "%s": %s', username, exc)
        return False, 'Неверный логин/пароль AD или учетная запись недоступна'
    except Exception as exc:
        app.logger.exception('Unexpected auth error for "%s": %s', username, exc)
        return False, 'Временная ошибка LDAP. Попробуйте позже'


@app.route('/')
def index():
    if not session.get('logged_in'): return redirect(url_for('login_page'))
    username = session.get('username')
    is_admin = username in MASTER_ADMINS
    can_manage_resources_flag = can_manage_resources(username)
    return render_template(
        'index.html',
        is_admin=is_admin,
        can_manage_resources=can_manage_resources_flag,
        user=username
    )


@app.route('/login', methods=['GET', 'POST'])
def login_page():
    if request.method == 'GET':
        if session.get('logged_in'): return redirect(url_for('index'))
        return render_template('login.html')
    data = request.json
    u = normalize_ad_username(data.get('username', ''))
    p = data.get('password', '')
    if not u or not p:
        return jsonify(success=False, error='Введите логин и пароль'), 400
    ok, auth_error = check_ldap_auth(u, p)
    if ok:
        session['logged_in'] = True
        session['username'] = u
        return jsonify(success=True)
    return jsonify(success=False, error=auth_error or 'Ошибка авторизации AD'), 401


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
    username = session.get('username')
    is_admin = username in MASTER_ADMINS
    can_view_private_phones = can_view_extended_phonebook(username)
    contacts = get_phonebook_contacts()
    return render_template(
        'phonebook.html',
        is_admin=is_admin,
        user=username,
        contacts=contacts,
        can_view_private_phones=can_view_private_phones
    )


@app.route('/meeting-rooms')
def meeting_rooms_page():
    if not session.get('logged_in'):
        return redirect(url_for('login_page'))
    is_admin = session.get('username') in MASTER_ADMINS
    return render_template(
        'meeting_rooms.html',
        is_admin=is_admin,
        user_login=session.get('username'),
        user_display=session.get('display_name') or session.get('username'),
        single_room_mode=False,
        fixed_room_name=''
    )


@app.route('/gym-booking')
def gym_booking_page():
    if not session.get('logged_in'):
        return redirect(url_for('login_page'))
    ensure_gym_room_exists()
    is_admin = session.get('username') in MASTER_ADMINS
    return render_template(
        'meeting_rooms.html',
        is_admin=is_admin,
        user_login=session.get('username'),
        user_display=session.get('display_name') or session.get('username'),
        single_room_mode=True,
        fixed_room_name=GYM_ROOM_NAME
    )


@app.route('/driver-trips')
def driver_trips_page():
    if not session.get('logged_in'):
        return redirect(url_for('login_page'))
    current_user = session.get('username')
    return render_template(
        'driver_trips.html',
        user_login=current_user,
        user_display=session.get('display_name') or current_user,
        can_manage_all=can_manage_all_bookings(current_user)
    )


@app.route('/knowledge-base')
def knowledge_base_page():
    if not session.get('logged_in'):
        return redirect(url_for('login_page'))
    categories = _knowledge_base_collect_categories()
    return render_template(
        'knowledge_base.html',
        user=session.get('username'),
        categories=categories
    )


@app.route('/tabel')
def tabel_page():
    if not session.get('logged_in'):
        return redirect(url_for('login_page'))
    ensure_tabel_index()
    now = datetime.now()
    current_period_key = f"{str(now.year)[2:]}_{now.month:02d}"
    with TABEL_INDEX_LOCK:
        all_periods = set()
        for dep_data in TABEL_INDEX.values():
            all_periods.update(dep_data.keys())
        periods = sorted(list(all_periods), reverse=True)
    human_periods = [{"key": p, "label": f"{p.split('_')[1]}.20{p.split('_')[0]}"} for p in periods if '_' in p]
    if not human_periods:
        for offset in range(12):
            month_index = now.month - offset
            year = now.year
            while month_index <= 0:
                month_index += 12
                year -= 1
            yy = str(year)[2:]
            mm = f'{month_index:02d}'
            human_periods.append({"key": f"{yy}_{mm}", "label": f"{mm}.{year}"})
    return render_template(
        'tabel.html',
        user=session.get('username'),
        user_display=session.get('display_name') or session.get('username'),
        periods=human_periods,
        leaders=get_tabel_leaders_data(),
        current_period=current_period_key
    )


@app.route('/logout')
def logout():
    login = normalize_ad_username(session.get('username') or '')
    if login in _ad_groups_cache:
        _ad_groups_cache.pop(login, None)
    session.clear()
    return redirect(url_for('login_page'))


@app.route('/api/tabel/meta')
def tabel_meta():
    if not session.get('logged_in'):
        return jsonify(success=False, error='Требуется авторизация'), 403
    ensure_tabel_index()
    now = datetime.now()
    source_available = os.path.exists(TABEL_BASE_DIR)
    leaders_source_available = os.path.exists(TABEL_LEADERS_FILE)
    current_period = f'{str(now.year)[2:]}_{now.month:02d}'
    with TABEL_INDEX_LOCK:
        all_periods = set()
        for dep_data in TABEL_INDEX.values():
            all_periods.update(dep_data.keys())
    periods = sorted(all_periods, reverse=True)
    payload = []
    for period in periods:
        if '_' not in period:
            continue
        yy, mm = period.split('_', 1)
        if not (yy.isdigit() and mm.isdigit()):
            continue
        payload.append({'key': period, 'label': f'{mm}.20{yy}'})
    # Если файлов табеля нет/недоступны, отдаем последние 12 месяцев как fallback,
    # чтобы UI оставался рабочим и предсказуемым для пользователя.
    if not payload:
        now = datetime.now()
        for offset in range(12):
            month_index = now.month - offset
            year = now.year
            while month_index <= 0:
                month_index += 12
                year -= 1
            yy = str(year)[2:]
            mm = f'{month_index:02d}'
            payload.append({'key': f'{yy}_{mm}', 'label': f'{mm}.{year}'})
    return jsonify({
        'periods': payload,
        'current_period': current_period,
        'source': {
            'base_dir': TABEL_BASE_DIR,
            'leaders_file': TABEL_LEADERS_FILE,
            'base_dir_available': source_available,
            'leaders_file_available': leaders_source_available
        }
    })


@app.route('/api/tabel/leaders')
def tabel_leaders():
    if not session.get('logged_in'):
        return jsonify(success=False, error='Требуется авторизация'), 403
    ensure_tabel_index()
    return jsonify(get_tabel_leaders_data())


@app.route('/api/tabel/search-fio')
def tabel_search_fio():
    if not session.get('logged_in'):
        return jsonify([]), 403
    ensure_tabel_index()
    query = (request.args.get('q') or '').strip().lower()
    if not query:
        return jsonify([])
    suggestions = set()
    with TABEL_INDEX_LOCK:
        for dep_data in TABEL_INDEX.values():
            for records in dep_data.values():
                for record in records:
                    for employee in record.get('employees', []):
                        fio = employee.get('fio') or ''
                        if query in fio.lower():
                            suggestions.add(fio)
    return jsonify(sorted(list(suggestions))[:15])


@app.route('/api/tabel/status-month')
def tabel_status_month():
    if not session.get('logged_in'):
        return jsonify(success=False, error='Требуется авторизация'), 403
    ensure_tabel_index()
    fio = (request.args.get('fio') or '').strip()
    period = (request.args.get('period') or '').strip()
    if not fio or not period or '_' not in period:
        return jsonify({'error': 'invalid'}), 400
    yy, mm = period.split('_', 1)
    try:
        target_year = 2000 + int(yy)
        target_month = int(mm)
        days_in_month = calendar.monthrange(target_year, target_month)[1]
    except Exception:
        return jsonify({'error': 'date'}), 400
    now = datetime.now()
    with TABEL_INDEX_LOCK:
        for dept_name, dep_periods in TABEL_INDEX.items():
            records = dep_periods.get(period) or []
            for record in records:
                match = next((emp for emp in record.get('employees', []) if emp.get('fio') == fio), None)
                if not match:
                    continue
                result = []
                days = match.get('days') or []
                for day_number in range(1, days_in_month + 1):
                    is_future = (
                        target_year > now.year or
                        (target_year == now.year and target_month > now.month) or
                        (target_year == now.year and target_month == now.month and day_number > now.day)
                    )
                    day_idx = day_number - 1
                    value = str(days[day_idx]).strip().upper() if day_idx < len(days) else ''
                    if value in ('', '0', '0.0'):
                        label, color = '—', 'red'
                    elif _tabel_is_work_value(value):
                        label, color = 'На работе', 'green'
                    elif value == 'В':
                        label, color = 'Выходной', 'orange'
                    else:
                        label, color = TABEL_STATUS_MAP.get(value, value), 'red'
                    result.append({'day': day_number, 'label': label, 'color': 'future' if is_future else color})
                return jsonify({'days': result, 'fio': fio, 'department': dept_name})
    return jsonify({'error': 'not_found'}), 404


@app.route('/api/leaders')
def tabel_leaders_compat():
    if not session.get('logged_in'):
        return jsonify([]), 403
    ensure_tabel_index()
    return jsonify(get_tabel_leaders_data())


@app.route('/search_fio')
def tabel_search_fio_compat():
    if not session.get('logged_in'):
        return jsonify([]), 403
    return tabel_search_fio()


@app.route('/status_month')
def tabel_status_month_compat():
    if not session.get('logged_in'):
        return jsonify({'error': 'unauthorized'}), 403
    return tabel_status_month()


@app.route('/api/knowledge-base/categories')
def knowledge_base_categories():
    if not session.get('logged_in'):
        return jsonify([]), 403
    payload = []
    for category in _knowledge_base_collect_categories():
        payload.append({
            'name': category,
            'files_count': len(_knowledge_base_collect_files(category))
        })
    return jsonify(payload)


@app.route('/api/knowledge-base/files')
def knowledge_base_files():
    if not session.get('logged_in'):
        return jsonify([]), 403
    category = (request.args.get('category') or '').strip()
    if not category:
        return jsonify([]), 400
    return jsonify(_knowledge_base_collect_files(category))


@app.route('/api/knowledge-base/search')
def knowledge_base_search():
    if not session.get('logged_in'):
        return jsonify([]), 403
    query = (request.args.get('q') or '').strip().lower()
    all_items = _knowledge_base_collect_all_files()
    if not query:
        return jsonify(all_items)
    filtered = []
    for item in all_items:
        haystack = f"{item.get('category', '')} {item.get('path', '')} {item.get('name', '')}".lower()
        if query in haystack:
            filtered.append(item)
    return jsonify(filtered)


@app.route('/knowledge-base/view/<path:category_name>/<path:file_path>')
def knowledge_base_view_file(category_name, file_path):
    if not session.get('logged_in'):
        return redirect(url_for('login_page'))
    target_path = _knowledge_base_resolve_file_path(category_name, file_path)
    if not target_path:
        return jsonify(success=False, error='Файл не найден'), 404
    return send_file(target_path, mimetype='application/pdf')


@app.route('/knowledge-base/download/<path:category_name>/<path:file_path>')
def knowledge_base_download_file(category_name, file_path):
    if not session.get('logged_in'):
        return redirect(url_for('login_page'))
    target_path = _knowledge_base_resolve_file_path(category_name, file_path)
    if not target_path:
        return jsonify(success=False, error='Файл не найден'), 404
    return send_file(target_path, as_attachment=True, download_name=os.path.basename(target_path))


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
          AND COALESCE(booking_status, 'active') = 'active'
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


def _is_driver_trip_in_past(trip_date, departure_time):
    try:
        trip_start = datetime.strptime(f"{trip_date} {departure_time}", "%Y-%m-%d %H:%M")
    except (TypeError, ValueError):
        return False
    return trip_start < datetime.now()


def _driver_trip_has_conflict(conn, payload, trip_id=None):
    """
    Конфликт для одного авто считаем по фиксированному окну 2 часа от времени выезда.
    """
    try:
        start_dt = datetime.strptime(f"{payload['trip_date']} {payload['departure_time']}", "%Y-%m-%d %H:%M")
    except (TypeError, ValueError):
        return False
    end_dt = start_dt + timedelta(hours=2)
    start_time = start_dt.strftime("%H:%M")
    end_time = end_dt.strftime("%H:%M")
    params = [
        payload['vehicle_model'].strip().lower(),
        payload['trip_date'],
        end_time,
        start_time
    ]
    query = '''
        SELECT 1
        FROM driver_trips
        WHERE LOWER(TRIM(vehicle_model)) = ?
          AND trip_date = ?
          AND COALESCE(trip_status, 'active') = 'active'
          AND NOT (? <= departure_time OR ? >= time(departure_time, '+2 hours'))
    '''
    if trip_id is not None:
        query += ' AND id <> ?'
        params.append(trip_id)
    query += ' LIMIT 1'
    return conn.execute(query, tuple(params)).fetchone() is not None


def _get_booked_by_display_name():
    return (session.get('display_name') or session.get('username') or '').strip()


def _normalize_participants(participants):
    if not isinstance(participants, list):
        return []
    normalized = []
    seen_logins = set()
    for item in participants:
        if not isinstance(item, dict):
            continue
        login = str(item.get('login') or '').strip().lower()
        name = str(item.get('name') or '').strip()
        if not login or not name or login in seen_logins:
            continue
        seen_logins.add(login)
        normalized.append({'login': login, 'name': name})
    return normalized


def _decode_participants(raw_value):
    if not raw_value:
        return []
    try:
        parsed = json.loads(raw_value)
    except (json.JSONDecodeError, TypeError):
        return []
    return _normalize_participants(parsed)


def _serialize_booking_state(booking_row):
    if not booking_row:
        return {}
    item = dict(booking_row)
    item['participants'] = _decode_participants(item.get('participants_json'))
    item.pop('participants_json', None)
    return item


def _log_booking_history(conn, booking_id, action, changed_by, details_payload):
    safe_details = json.dumps(details_payload or {}, ensure_ascii=False)
    conn.execute(
        '''
        INSERT INTO meeting_booking_history (booking_id, action, changed_by, changed_at, details_json)
        VALUES (?, ?, ?, ?, ?)
        ''',
        (
            booking_id,
            action,
            (changed_by or '').strip().lower(),
            datetime.now(APP_TZ).strftime('%Y-%m-%d %H:%M:%S'),
            safe_details
        )
    )


def _serialize_driver_trip_state(trip_row):
    if not trip_row:
        return {}
    return dict(trip_row)


def _log_driver_trip_history(conn, trip_id, action, changed_by, details_payload):
    safe_details = json.dumps(details_payload or {}, ensure_ascii=False)
    conn.execute(
        '''
        INSERT INTO driver_trip_history (trip_id, action, changed_by, changed_at, details_json)
        VALUES (?, ?, ?, ?, ?)
        ''',
        (
            trip_id,
            action,
            (changed_by or '').strip().lower(),
            datetime.now(APP_TZ).strftime('%Y-%m-%d %H:%M:%S'),
            safe_details
        )
    )


def _username_to_corporate_email(username):
    login = (username or '').strip().lower()
    if not login:
        return ''
    return f'{login}@{MAIL_DOMAIN}'


def _send_meeting_cancellation_email(recipient_email, booking_info):
    if not recipient_email:
        return False, 'Не указан email получателя'
    meeting_date = booking_info.get('meeting_date') or ''
    start_time = booking_info.get('start_time') or ''
    end_time = booking_info.get('end_time') or ''
    room_name = booking_info.get('room_name') or ''
    purpose = booking_info.get('purpose') or ''
    canceled_by = booking_info.get('canceled_by') or 'администратор'
    sent_at_minsk = datetime.now(APP_TZ)
    sent_at_text = sent_at_minsk.strftime('%d.%m.%Y %H:%M')
    subject = f'[{sent_at_text} РБ] Отмена встречи: {purpose or "без темы"}'
    body = (
        'Здравствуйте.\n\n'
        'Ваша встреча была отменена администратором.\n\n'
        f'Дата: {meeting_date}\n'
        f'Время: {start_time} - {end_time}\n'
        f'Переговорка: {room_name}\n'
        f'Тема: {purpose}\n'
        f'Кто отменил: {canceled_by}\n'
        f'Время отправки (РБ): {sent_at_text}\n\n'
        'Сообщение отправлено автоматически.'
    )
    message = EmailMessage(policy=policy.SMTPUTF8)
    message['Subject'] = str(Header(subject, 'utf-8'))
    message['From'] = MAIL_SENDER
    message['To'] = recipient_email
    message['Date'] = format_datetime(sent_at_minsk)
    # Принудительно отправляем тело в UTF-8 Base64 для стабильной кириллицы в разных клиентах.
    message.set_content(body, subtype='plain', charset='utf-8', cte='base64')
    try:
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=10) as smtp:
            if SMTP_USE_TLS:
                smtp.starttls()
            if SMTP_USERNAME:
                smtp.login(SMTP_USERNAME, SMTP_PASSWORD)
            smtp.send_message(message)
        return True, ''
    except Exception as exc:
        return False, str(exc)


@app.route('/api/meeting-bookings')
def get_meeting_bookings():
    if not session.get('logged_in'):
        return jsonify([]), 403
    conn = get_db_connection()
    try:
        rows = conn.execute('''
            SELECT
                b.id, b.room_id, r.name AS room_name, b.booked_by, b.purpose,
                b.participants_json, b.meeting_date, b.start_time, b.end_time,
                b.booking_status, b.canceled_by, b.canceled_at, b.owner_username
            FROM meeting_bookings b
            JOIN meeting_rooms r ON r.id = b.room_id
            ORDER BY b.meeting_date ASC, b.start_time ASC
        ''').fetchall()
        data = []
        for row in rows:
            item = dict(row)
            item['participants'] = _decode_participants(item.get('participants_json'))
            item.pop('participants_json', None)
            data.append(item)
        return jsonify(data)
    finally:
        conn.close()


@app.route('/api/meeting-bookings', methods=['POST'])
def create_meeting_booking():
    if not session.get('logged_in'):
        return jsonify(success=False), 403
    data = request.json or {}
    payload = {
        'room_id': data.get('room_id'),
        'purpose': (data.get('purpose') or '').strip(),
        'participants': _normalize_participants(data.get('participants')),
        'meeting_date': (data.get('meeting_date') or '').strip(),
        'start_time': (data.get('start_time') or '').strip(),
        'end_time': (data.get('end_time') or '').strip(),
    }
    if not all([payload['room_id'], payload['purpose'], payload['meeting_date'],
                payload['start_time'], payload['end_time']]):
        return jsonify(success=False, error='Заполните все поля бронирования'), 400
    if not payload['participants']:
        return jsonify(success=False, error='Добавьте хотя бы одного участника из AD'), 400
    booked_by = _get_booked_by_display_name()
    if not booked_by:
        return jsonify(success=False, error='Не удалось определить текущего пользователя AD'), 400
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
                room_id, booked_by, purpose, participants_json, meeting_date, start_time, end_time, owner_username
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            payload['room_id'], booked_by, payload['purpose'], json.dumps(payload['participants'], ensure_ascii=False),
            payload['meeting_date'], payload['start_time'], payload['end_time'], session.get('username')
        ))
        booking_id = conn.execute('SELECT last_insert_rowid() AS id').fetchone()['id']
        created_row = conn.execute('''
            SELECT
                b.id, b.room_id, r.name AS room_name, b.booked_by, b.purpose,
                b.participants_json, b.meeting_date, b.start_time, b.end_time,
                b.booking_status, b.canceled_by, b.canceled_at, b.owner_username
            FROM meeting_bookings b
            JOIN meeting_rooms r ON r.id = b.room_id
            WHERE b.id = ?
        ''', (booking_id,)).fetchone()
        _log_booking_history(conn, booking_id, 'created', session.get('username'), {
            'after': _serialize_booking_state(created_row)
        })
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
        'purpose': (data.get('purpose') or '').strip(),
        'participants': _normalize_participants(data.get('participants')),
        'meeting_date': (data.get('meeting_date') or '').strip(),
        'start_time': (data.get('start_time') or '').strip(),
        'end_time': (data.get('end_time') or '').strip(),
    }
    if not all([payload['room_id'], payload['purpose'], payload['meeting_date'],
                payload['start_time'], payload['end_time']]):
        return jsonify(success=False, error='Заполните все поля бронирования'), 400
    if not payload['participants']:
        return jsonify(success=False, error='Добавьте хотя бы одного участника из AD'), 400
    if payload['end_time'] <= payload['start_time']:
        return jsonify(success=False, error='Время окончания должно быть больше времени начала'), 400
    if _is_booking_in_past(payload):
        return jsonify(success=False, error='Нельзя сохранять бронь на прошедшие дату и время'), 400
    conn = get_db_connection()
    try:
        booking = conn.execute('''
            SELECT
                b.id, b.room_id, r.name AS room_name, b.booked_by, b.purpose,
                b.participants_json, b.meeting_date, b.start_time, b.end_time,
                COALESCE(b.booking_status, 'active') AS booking_status,
                b.canceled_by, b.canceled_at, b.owner_username
            FROM meeting_bookings b
            JOIN meeting_rooms r ON r.id = b.room_id
            WHERE b.id = ?
        ''', (booking_id,)).fetchone()
        if not booking:
            return jsonify(success=False, error='Бронь не найдена'), 404
        if booking['booking_status'] == 'canceled':
            return jsonify(success=False, error='Отмененную бронь редактировать нельзя'), 409
        current_user = session.get('username')
        if not can_manage_all_bookings(current_user) and booking['owner_username'] != current_user:
            return jsonify(success=False, error='Редактировать можно только свои брони'), 403
        room_exists = conn.execute('SELECT 1 FROM meeting_rooms WHERE id = ?', (payload['room_id'],)).fetchone()
        if not room_exists:
            return jsonify(success=False, error='Выбранная переговорка не найдена'), 404
        if _meeting_has_conflict(conn, payload, booking_id=booking_id):
            return jsonify(success=False, error='На выбранное время переговорка уже занята'), 409
        conn.execute('''
            UPDATE meeting_bookings
            SET room_id = ?, purpose = ?, participants_json = ?, meeting_date = ?, start_time = ?, end_time = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
        ''', (
            payload['room_id'], payload['purpose'], json.dumps(payload['participants'], ensure_ascii=False),
            payload['meeting_date'], payload['start_time'], payload['end_time'], booking_id
        ))
        updated_row = conn.execute('''
            SELECT
                b.id, b.room_id, r.name AS room_name, b.booked_by, b.purpose,
                b.participants_json, b.meeting_date, b.start_time, b.end_time,
                COALESCE(b.booking_status, 'active') AS booking_status,
                b.canceled_by, b.canceled_at, b.owner_username
            FROM meeting_bookings b
            JOIN meeting_rooms r ON r.id = b.room_id
            WHERE b.id = ?
        ''', (booking_id,)).fetchone()
        _log_booking_history(conn, booking_id, 'updated', current_user, {
            'before': _serialize_booking_state(booking),
            'after': _serialize_booking_state(updated_row)
        })
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
        booking = conn.execute('''
            SELECT
                b.id, b.room_id, r.name AS room_name, b.booked_by, b.purpose,
                b.participants_json, b.meeting_date, b.start_time, b.end_time,
                COALESCE(b.booking_status, 'active') AS booking_status,
                b.canceled_by, b.canceled_at, b.owner_username
            FROM meeting_bookings b
            JOIN meeting_rooms r ON r.id = b.room_id
            WHERE b.id = ?
        ''', (booking_id,)).fetchone()
        if not booking:
            return jsonify(success=False, error='Бронь не найдена'), 404
        current_user = session.get('username')
        if not can_manage_all_bookings(current_user) and booking['owner_username'] != current_user:
            return jsonify(success=False, error='Можно отменять только свои брони'), 403
        if booking['booking_status'] == 'canceled':
            return jsonify(success=False, error='Бронь уже отменена'), 409
        conn.execute('''
            UPDATE meeting_bookings
            SET booking_status = 'canceled',
                canceled_by = ?,
                canceled_at = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
        ''', (current_user, datetime.now(APP_TZ).strftime('%Y-%m-%d %H:%M:%S'), booking_id))
        booking_info = conn.execute('''
            SELECT b.meeting_date, b.start_time, b.end_time, b.purpose,
                   r.name AS room_name, b.owner_username
            FROM meeting_bookings b
            JOIN meeting_rooms r ON r.id = b.room_id
            WHERE b.id = ?
        ''', (booking_id,)).fetchone()
        canceled_row = conn.execute('''
            SELECT
                b.id, b.room_id, r.name AS room_name, b.booked_by, b.purpose,
                b.participants_json, b.meeting_date, b.start_time, b.end_time,
                COALESCE(b.booking_status, 'active') AS booking_status,
                b.canceled_by, b.canceled_at, b.owner_username
            FROM meeting_bookings b
            JOIN meeting_rooms r ON r.id = b.room_id
            WHERE b.id = ?
        ''', (booking_id,)).fetchone()
        _log_booking_history(conn, booking_id, 'canceled', current_user, {
            'before': _serialize_booking_state(booking),
            'after': _serialize_booking_state(canceled_row)
        })
        conn.commit()
        email_warning = ''
        should_notify_owner = (
            booking_info is not None and
            can_manage_all_bookings(current_user) and
            booking_info['owner_username'] != current_user
        )
        if should_notify_owner:
            owner_email = _username_to_corporate_email(booking_info['owner_username'])
            ok, error_text = _send_meeting_cancellation_email(owner_email, {
                'meeting_date': booking_info['meeting_date'],
                'start_time': booking_info['start_time'],
                'end_time': booking_info['end_time'],
                'purpose': booking_info['purpose'],
                'room_name': booking_info['room_name'],
                'canceled_by': current_user
            })
            if not ok:
                email_warning = f'Не удалось отправить уведомление: {error_text}'
        return jsonify(success=True, status='canceled', warning=email_warning)
    finally:
        conn.close()


@app.route('/api/meeting-bookings/<int:booking_id>/history')
def get_meeting_booking_history(booking_id):
    if not session.get('logged_in'):
        return jsonify([]), 403
    conn = get_db_connection()
    try:
        booking_exists = conn.execute('SELECT 1 FROM meeting_bookings WHERE id = ?', (booking_id,)).fetchone()
        if not booking_exists:
            return jsonify([]), 404
        rows = conn.execute('''
            SELECT id, booking_id, action, changed_by, changed_at, details_json
            FROM meeting_booking_history
            WHERE booking_id = ?
            ORDER BY id DESC
        ''', (booking_id,)).fetchall()
        data = []
        for row in rows:
            item = dict(row)
            try:
                item['details'] = json.loads(item.get('details_json') or '{}')
            except (TypeError, json.JSONDecodeError):
                item['details'] = {}
            item.pop('details_json', None)
            data.append(item)
        return jsonify(data)
    finally:
        conn.close()


@app.route('/api/driver-trips')
def get_driver_trips():
    if not session.get('logged_in'):
        return jsonify([]), 403
    conn = get_db_connection()
    try:
        rows = conn.execute(
            '''
            SELECT id, vehicle_model, vehicle_color, trip_date, departure_time, origin, destination, description,
                   created_by, owner_username, trip_status, canceled_by, canceled_at
            FROM driver_trips
            ORDER BY trip_date ASC, departure_time ASC
            '''
        ).fetchall()
        return jsonify([dict(row) for row in rows])
    finally:
        conn.close()


@app.route('/api/driver-trips', methods=['POST'])
def create_driver_trip():
    if not session.get('logged_in'):
        return jsonify(success=False), 403
    data = request.json or {}
    payload = {
        'vehicle_model': (data.get('vehicle_model') or '').strip(),
        'vehicle_color': (data.get('vehicle_color') or '#1f77b4').strip(),
        'trip_date': (data.get('trip_date') or '').strip(),
        'departure_time': (data.get('departure_time') or '').strip(),
        'origin': (data.get('origin') or 'РУП «Белнипиэнергопром»').strip() or 'РУП «Белнипиэнергопром»',
        'destination': (data.get('destination') or '').strip(),
        'description': (data.get('description') or '').strip()
    }
    if not all([payload['vehicle_model'], payload['trip_date'], payload['departure_time'], payload['destination']]):
        return jsonify(success=False, error='Заполните обязательные поля рейса'), 400
    try:
        datetime.strptime(payload['trip_date'], '%Y-%m-%d')
        datetime.strptime(payload['departure_time'], '%H:%M')
    except ValueError:
        return jsonify(success=False, error='Некорректная дата или время'), 400
    if not re.match(r'^#[0-9a-fA-F]{6}$', payload['vehicle_color']):
        return jsonify(success=False, error='Некорректный цвет автомобиля'), 400
    if _is_driver_trip_in_past(payload['trip_date'], payload['departure_time']):
        return jsonify(success=False, error='Нельзя создавать рейс на прошедшие дату и время'), 400
    conn = get_db_connection()
    try:
        if _driver_trip_has_conflict(conn, payload):
            return jsonify(success=False, error='Для выбранного авто есть пересечение по времени'), 409
        conn.execute(
            '''
            INSERT INTO driver_trips (
                vehicle_model, vehicle_color, trip_date, departure_time, origin, destination, description,
                owner_username, created_by
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            (
                payload['vehicle_model'],
                payload['vehicle_color'],
                payload['trip_date'],
                payload['departure_time'],
                payload['origin'],
                payload['destination'],
                payload['description'],
                session.get('username'),
                session.get('username')
            )
        )
        trip_id = conn.execute('SELECT last_insert_rowid() AS id').fetchone()['id']
        created_trip = conn.execute(
            '''
            SELECT id, vehicle_model, vehicle_color, trip_date, departure_time, origin, destination, description,
                   created_by, owner_username, trip_status, canceled_by, canceled_at
            FROM driver_trips
            WHERE id = ?
            ''',
            (trip_id,)
        ).fetchone()
        _log_driver_trip_history(conn, trip_id, 'created', session.get('username'), {
            'after': _serialize_driver_trip_state(created_trip)
        })
        conn.commit()
        return jsonify(success=True)
    finally:
        conn.close()


@app.route('/api/driver-trips/<int:trip_id>', methods=['PUT'])
def update_driver_trip(trip_id):
    if not session.get('logged_in'):
        return jsonify(success=False), 403
    data = request.json or {}
    payload = {
        'vehicle_model': (data.get('vehicle_model') or '').strip(),
        'vehicle_color': (data.get('vehicle_color') or '#1f77b4').strip(),
        'trip_date': (data.get('trip_date') or '').strip(),
        'departure_time': (data.get('departure_time') or '').strip(),
        'origin': (data.get('origin') or 'РУП «Белнипиэнергопром»').strip() or 'РУП «Белнипиэнергопром»',
        'destination': (data.get('destination') or '').strip(),
        'description': (data.get('description') or '').strip()
    }
    if not all([payload['vehicle_model'], payload['trip_date'], payload['departure_time'], payload['destination']]):
        return jsonify(success=False, error='Заполните обязательные поля рейса'), 400
    try:
        datetime.strptime(payload['trip_date'], '%Y-%m-%d')
        datetime.strptime(payload['departure_time'], '%H:%M')
    except ValueError:
        return jsonify(success=False, error='Некорректная дата или время'), 400
    if not re.match(r'^#[0-9a-fA-F]{6}$', payload['vehicle_color']):
        return jsonify(success=False, error='Некорректный цвет автомобиля'), 400
    if _is_driver_trip_in_past(payload['trip_date'], payload['departure_time']):
        return jsonify(success=False, error='Нельзя редактировать рейс на прошедшие дату и время'), 400
    current_user = session.get('username')
    conn = get_db_connection()
    try:
        trip = conn.execute(
            '''
            SELECT id, vehicle_model, vehicle_color, trip_date, departure_time, origin, destination, description,
                   created_by, owner_username, COALESCE(trip_status, 'active') AS trip_status, canceled_by, canceled_at
            FROM driver_trips
            WHERE id = ?
            ''',
            (trip_id,)
        ).fetchone()
        if not trip:
            return jsonify(success=False, error='Рейс не найден'), 404
        owner_login = trip['owner_username'] or trip['created_by']
        if not can_manage_all_bookings(current_user) and owner_login != current_user:
            return jsonify(success=False, error='Редактировать можно только свои рейсы'), 403
        if trip['trip_status'] == 'canceled':
            return jsonify(success=False, error='Отмененный рейс редактировать нельзя'), 409
        if _driver_trip_has_conflict(conn, payload, trip_id=trip_id):
            return jsonify(success=False, error='Для выбранного авто есть пересечение по времени'), 409
        conn.execute(
            '''
            UPDATE driver_trips
            SET vehicle_model = ?, vehicle_color = ?, trip_date = ?, departure_time = ?, origin = ?, destination = ?,
                description = ?, updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
            ''',
            (
                payload['vehicle_model'],
                payload['vehicle_color'],
                payload['trip_date'],
                payload['departure_time'],
                payload['origin'],
                payload['destination'],
                payload['description'],
                trip_id
            )
        )
        updated_trip = conn.execute(
            '''
            SELECT id, vehicle_model, vehicle_color, trip_date, departure_time, origin, destination, description,
                   created_by, owner_username, COALESCE(trip_status, 'active') AS trip_status, canceled_by, canceled_at
            FROM driver_trips
            WHERE id = ?
            ''',
            (trip_id,)
        ).fetchone()
        _log_driver_trip_history(conn, trip_id, 'updated', current_user, {
            'before': _serialize_driver_trip_state(trip),
            'after': _serialize_driver_trip_state(updated_trip)
        })
        conn.commit()
        return jsonify(success=True)
    finally:
        conn.close()


@app.route('/api/driver-trips/<int:trip_id>', methods=['DELETE'])
def cancel_driver_trip(trip_id):
    if not session.get('logged_in'):
        return jsonify(success=False), 403
    current_user = session.get('username')
    conn = get_db_connection()
    try:
        trip = conn.execute(
            '''
            SELECT id, vehicle_model, vehicle_color, trip_date, departure_time, origin, destination, description,
                   created_by, owner_username, COALESCE(trip_status, 'active') AS trip_status, canceled_by, canceled_at
            FROM driver_trips
            WHERE id = ?
            ''',
            (trip_id,)
        ).fetchone()
        if not trip:
            return jsonify(success=False, error='Рейс не найден'), 404
        owner_login = trip['owner_username'] or trip['created_by']
        if not can_manage_all_bookings(current_user) and owner_login != current_user:
            return jsonify(success=False, error='Можно отменять только свои рейсы'), 403
        if trip['trip_status'] == 'canceled':
            return jsonify(success=False, error='Рейс уже отменен'), 409
        conn.execute(
            '''
            UPDATE driver_trips
            SET trip_status = 'canceled',
                canceled_by = ?,
                canceled_at = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
            ''',
            (current_user, datetime.now(APP_TZ).strftime('%Y-%m-%d %H:%M:%S'), trip_id)
        )
        canceled_trip = conn.execute(
            '''
            SELECT id, vehicle_model, vehicle_color, trip_date, departure_time, origin, destination, description,
                   created_by, owner_username, COALESCE(trip_status, 'active') AS trip_status, canceled_by, canceled_at
            FROM driver_trips
            WHERE id = ?
            ''',
            (trip_id,)
        ).fetchone()
        _log_driver_trip_history(conn, trip_id, 'canceled', current_user, {
            'before': _serialize_driver_trip_state(trip),
            'after': _serialize_driver_trip_state(canceled_trip)
        })
        conn.commit()
        return jsonify(success=True, status='canceled')
    finally:
        conn.close()


@app.route('/api/driver-trips/<int:trip_id>/history')
def get_driver_trip_history(trip_id):
    if not session.get('logged_in'):
        return jsonify([]), 403
    conn = get_db_connection()
    try:
        trip_exists = conn.execute('SELECT 1 FROM driver_trips WHERE id = ?', (trip_id,)).fetchone()
        if not trip_exists:
            return jsonify([]), 404
        rows = conn.execute(
            '''
            SELECT id, trip_id, action, changed_by, changed_at, details_json
            FROM driver_trip_history
            WHERE trip_id = ?
            ORDER BY id DESC
            ''',
            (trip_id,)
        ).fetchall()
        data = []
        for row in rows:
            item = dict(row)
            try:
                item['details'] = json.loads(item.get('details_json') or '{}')
            except (TypeError, json.JSONDecodeError):
                item['details'] = {}
            item.pop('details_json', None)
            data.append(item)
        return jsonify(data)
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
    user_ad_groups = get_user_ad_groups_by_username(u)
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
            for row in admin_rows:
                row['url'] = normalize_resource_url(row.get('url'))
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
            row_dict['url'] = normalize_resource_url(row_dict.get('url'))
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
    if not can_manage_resources(session.get('username')): return jsonify(success=False), 403
    t = request.form.get('title')
    u = normalize_resource_url(request.form.get('url'))
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
    if not can_manage_resources(session.get('username')): return jsonify(success=False), 403
    t = request.form.get('title')
    u = normalize_resource_url(request.form.get('url'))
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
    if not can_manage_resources(session.get('username')): return jsonify(success=False), 403
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
    if not can_manage_resources(session.get('username')): return jsonify(success=False), 403
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


@app.route('/api/phonebook-access')
def get_phonebook_access_users():
    if session.get('username') not in MASTER_ADMINS:
        return jsonify([]), 403
    conn = get_db_connection()
    try:
        rows = conn.execute(
            '''
            SELECT entity_type, entity_login
            FROM phonebook_privileged_entities
            ORDER BY entity_type ASC, entity_login COLLATE NOCASE
            '''
        ).fetchall()
        payload = [
            {'type': row['entity_type'], 'login': row['entity_login']}
            for row in rows
        ]
        return jsonify(payload)
    finally:
        conn.close()


@app.route('/manage_phonebook_access', methods=['POST'])
def manage_phonebook_access():
    if session.get('username') not in MASTER_ADMINS:
        return jsonify(success=False), 403
    payload = request.json or {}
    action = (payload.get('action') or '').strip()
    entity_type = (payload.get('type') or 'user').strip().lower()
    raw_login = payload.get('username')
    if entity_type == 'group':
        entity_login = str(raw_login or '').strip().lower()
    else:
        entity_type = 'user'
        entity_login = normalize_ad_username(raw_login)
    if action not in ('add', 'delete'):
        return jsonify(success=False, error='Неизвестное действие'), 400
    if not entity_login:
        return jsonify(success=False, error='Укажите пользователя'), 400
    conn = get_db_connection()
    try:
        if action == 'add':
            conn.execute(
                'INSERT OR IGNORE INTO phonebook_privileged_entities (entity_type, entity_login) VALUES (?, ?)',
                (entity_type, entity_login)
            )
        else:
            conn.execute(
                'DELETE FROM phonebook_privileged_entities WHERE entity_type = ? AND entity_login = ?',
                (entity_type, entity_login)
            )
        conn.commit()
        return jsonify(success=True)
    finally:
        conn.close()


@app.route('/api/booking-access')
def get_booking_access_entities():
    if session.get('username') not in MASTER_ADMINS:
        return jsonify([]), 403
    conn = get_db_connection()
    try:
        rows = conn.execute(
            '''
            SELECT entity_type, entity_login
            FROM booking_privileged_entities
            ORDER BY entity_type ASC, entity_login COLLATE NOCASE
            '''
        ).fetchall()
        return jsonify([{'type': row['entity_type'], 'login': row['entity_login']} for row in rows])
    finally:
        conn.close()


@app.route('/manage_booking_access', methods=['POST'])
def manage_booking_access():
    if session.get('username') not in MASTER_ADMINS:
        return jsonify(success=False), 403
    payload = request.json or {}
    action = (payload.get('action') or '').strip()
    entity_type = (payload.get('type') or 'user').strip().lower()
    raw_login = payload.get('username')
    if entity_type == 'group':
        entity_login = str(raw_login or '').strip().lower()
    else:
        entity_type = 'user'
        entity_login = normalize_ad_username(raw_login)
    if action not in ('add', 'delete'):
        return jsonify(success=False, error='Неизвестное действие'), 400
    if not entity_login:
        return jsonify(success=False, error='Укажите пользователя или группу'), 400
    conn = get_db_connection()
    try:
        if action == 'add':
            conn.execute(
                'INSERT OR IGNORE INTO booking_privileged_entities (entity_type, entity_login) VALUES (?, ?)',
                (entity_type, entity_login)
            )
        else:
            conn.execute(
                'DELETE FROM booking_privileged_entities WHERE entity_type = ? AND entity_login = ?',
                (entity_type, entity_login)
            )
        conn.commit()
        return jsonify(success=True)
    finally:
        conn.close()


@app.route('/api/resource-access')
def get_resource_access_entities():
    if session.get('username') not in MASTER_ADMINS:
        return jsonify([]), 403
    conn = get_db_connection()
    try:
        rows = conn.execute(
            '''
            SELECT entity_type, entity_login
            FROM resource_privileged_entities
            ORDER BY entity_type ASC, entity_login COLLATE NOCASE
            '''
        ).fetchall()
        return jsonify([{'type': row['entity_type'], 'login': row['entity_login']} for row in rows])
    finally:
        conn.close()


@app.route('/manage_resource_access', methods=['POST'])
def manage_resource_access():
    if session.get('username') not in MASTER_ADMINS:
        return jsonify(success=False), 403
    payload = request.json or {}
    action = (payload.get('action') or '').strip()
    entity_type = (payload.get('type') or 'user').strip().lower()
    raw_login = payload.get('username')
    if entity_type == 'group':
        entity_login = str(raw_login or '').strip().lower()
    else:
        entity_type = 'user'
        entity_login = normalize_ad_username(raw_login)
    if action not in ('add', 'delete'):
        return jsonify(success=False, error='Неизвестное действие'), 400
    if not entity_login:
        return jsonify(success=False, error='Укажите пользователя или группу'), 400
    conn = get_db_connection()
    try:
        if action == 'add':
            conn.execute(
                'INSERT OR IGNORE INTO resource_privileged_entities (entity_type, entity_login) VALUES (?, ?)',
                (entity_type, entity_login)
            )
        else:
            conn.execute(
                'DELETE FROM resource_privileged_entities WHERE entity_type = ? AND entity_login = ?',
                (entity_type, entity_login)
            )
        conn.commit()
        return jsonify(success=True)
    finally:
        conn.close()


if __name__ == '__main__':
    os.makedirs(KNOWLEDGE_BASE_INSTRUCTIONS_DIR, exist_ok=True)
    init_db()
    _tabel_load_cache()
    _tabel_rebuild_index_from_cache()
    app.run(host='0.0.0.0', port=5004, debug=True, threaded=True)