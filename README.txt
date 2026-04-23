РУП БЕЛНИПИЭНЕРГОПРОМ
==========

Описание
--------
РУП «Белнипиэнергопром» — веб-портал на Flask для публикации внутренних ссылок и управления доступом по LDAP и группам.

Что делает проект
-----------------
- Авторизация через LDAP (Active Directory)
- Отображение ресурсов по категориям
- Ограничение видимости ресурсов по группам
- Админ-раздел для управления ресурсами и группами

Как поднять проект с нуля
-------------------------
1) Склонируйте репозиторий:
   git clone https://github.com/yelizavetafitil/PythonProject1111.git
   cd PythonProject1111

2) Создайте виртуальное окружение:
   Windows PowerShell:
   py -m venv .venv
   .\.venv\Scripts\Activate.ps1

   Linux/macOS:
   python3 -m venv .venv
   source .venv/bin/activate

3) Установите зависимости:
   pip install --upgrade pip
   pip install -r requirements.txt

4) Запустите приложение:
   python app.py

5) Откройте в браузере:
   http://127.0.0.1:5004

Настройка LDAP
--------------
Перед запуском проверьте LDAP-настройки в app.py, блок LDAP_CONFIG:
- uri
- base
- bind_dn
- bind_password
- user_attr

Важно
-----
- Папка .venv не передается в git.
- Окружение создается локально на каждой машине.
- Все библиотеки ставятся из requirements.txt.

Модуль "Табель" в едином сервисе
--------------------------------
Код табеля перенесен из отдельного проекта в этот Flask-сервис.
Основные маршруты:
- /tabel
- /api/tabel/meta
- /api/tabel/leaders
- /api/tabel/search-fio
- /api/tabel/status-month

Источник данных и кэш:
- TABEL_BASE_DIR (по умолчанию: \\srv-doc\ТАБЕЛЬ)
- TABEL_LEADERS_FILE (по умолчанию: \\srv-doc\ТАБЕЛЬ\ОЦ\Список руководителей.xlsx)
- TABEL_SHEET_NAME (по умолчанию: Табель_3)
- TABEL_CACHE_FILE (по умолчанию: tabel_portal_cache.json)

Для продакшна как единого сервиса:
1) Пользователи работают только через этот проект (/tabel).
2) Старый сервис табеля (PythonProject15) после приемки отключается.
3) Прямой доступ к старому сервису/порту закрывается firewall-правилом.
