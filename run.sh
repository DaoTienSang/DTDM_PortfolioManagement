#!/bin/bash

echo "=== CHECK PYTHON VERSION ==="
if ! python --version 2>&1 | grep -q "Python 3"; then
    echo "ERROR: Python3 is required!"
    read -p "Press Enter to continue..."
    exit 1
fi
echo "=== PYTHON VERSION CHECKED ==="

echo "=== LOAD ENVIRONMENT VARIABLES ==="
if [ -f .env ]; then
    export $(grep -v '^#' .env | grep -E '^[A-Za-z_][A-Za-z0-9_]*=.*$' | sed 's/[[:space:]]*$//' | xargs)
else
    echo "WARNING: .env file not found, relying on Docker environment variables"
fi
echo "=== LOAD ENVIRONMENT VARIABLES COMPLETELY ==="

echo "=== CHECK POSTGRES DATABASE ==="
if ! command -v psql >/dev/null 2>&1; then
    echo "ERROR: PostgreSQL is not installed!"
    read -p "Press Enter to continue..."
    exit 1
fi
echo "=== POSTGRES DATABASE IS INSTALLED ==="

echo "=== CONNECT TO DATABASE ==="
export PGPASSWORD="$DATABASE_PASSWORD"
psql -U "$DATABASE_USER" -h "$DATABASE_HOST" -p "$DATABASE_PORT" -c "SELECT 1;" >/dev/null 2>&1
if [ $? -ne 0 ]; then
    echo "ERROR: Cannot connect to PostgreSQL!"
    echo "Check PostgreSQL server and sign-in information!"
    read -p "Press Enter to continue..."
    exit 1
fi
echo "=== DATABASE IS READY ==="

echo "=== UPDATE METADATA OF DATABASE ==="
psql -U "$DATABASE_USER" -c "ALTER DATABASE $DATABASE_NAME REFRESH COLLATION VERSION;"
echo "=== UPDATE METADATA OF DATABASE COMPLETELY ==="

echo "=== CREATE VIRTUAL ENVIRONMENT ==="
if [ -d "venv" ]; then
    rm -rf venv
    sleep 2
fi
python -m venv venv || { echo "❌ Failed to create venv"; exit 1; }
if [ -f "venv/Scripts/activate" ]; then
    source venv/Scripts/activate
else
    echo ""❌ Activate script not found! venv creation may have failed.""
    exit 1
fi
if ! python --version >/dev/null 2>&1; then
    echo "ERROR: Failed to activate virtual environment!"
    read -p "Press Enter to continue..."
    exit 1
fi
pip install --upgrade pip
pip install wheel setuptools
if [ ! -f requirements.txt ]; then
    echo "ERROR: requirements.txt not found!"
    read -p "Press Enter to continue..."
    exit 1
fi
pip install -r requirements.txt
echo "=== CREATE VIRTUAL ENVIRONMENT COMPLETELY ==="

if [ ! -d src ]; then
    echo "ERROR: src directory not found!"
    read -p "Press Enter to continue..."
    exit 1
fi

echo "=== NAVIGATE TO PROJECT DIRECTORY ==="
cd src
mkdir -p static
mkdir -p media

echo "=== CREATE DATABASE ==="
psql -h "$DATABASE_HOST" -U "$DATABASE_USER" -p "$DATABASE_PORT" -c "DROP DATABASE IF EXISTS $DATABASE_NAME;"
psql -h "$DATABASE_HOST" -U "$DATABASE_USER" -p "$DATABASE_PORT" -c "CREATE DATABASE $DATABASE_NAME WITH ENCODING='UTF8' TEMPLATE=template0;"
echo "=== CREATE DATABASE COMPLETELY ==="

echo "=== CREATE MIGRATIONS ==="
rm -rf portfolio/migrations
mkdir -p portfolio/migrations
touch portfolio/migrations/__init__.py
python manage.py makemigrations
if [ $? -ne 0 ]; then
    echo "ERROR: Cannot create migrations"
    read -p "Press Enter to continue..."
    exit 1
fi
python manage.py migrate
if [ $? -ne 0 ]; then
    echo "ERROR: Cannot apply migrations"
    echo "Check PostgreSQL connection"
    read -p "Press Enter to continue..."
    exit 1
fi
echo "=== CREATE MIGRATIONS COMPLETELY ==="

echo "=== CREATE ADMIN ACCOUNT ==="
python manage.py shell -c "from django.contrib.auth import get_user_model; User = get_user_model(); User.objects.create_superuser('$admin_username', 'admin@example.com', '$admin_password') if not User.objects.filter(username='$admin_username').exists() else print('Admin already exists!!!')"
echo "=== CREATE ADMIN ACCOUNT COMPLETELY ==="

echo "=== RUN SERVER ==="
if [[ "$OSTYPE" == "darwin"* ]]; then
    open http://127.0.0.1:8000 || echo "WARNING: Could not open browser"
else
    xdg-open http://127.0.0.1:8000 >/dev/null 2>&1 || echo "WARNING: Could not open browser"
fi
python manage.py runserver