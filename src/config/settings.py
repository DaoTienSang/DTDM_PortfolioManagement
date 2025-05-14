import os
from pathlib import Path
from decouple import config
import socket



BASE_DIR = Path(__file__).resolve().parent.parent


# SECRET_KEY = config("SECRET_KEY")
# DEBUG = False if config('DEBUG')=="False" else True
# ALLOWED_HOSTS = config('ALLOWED_HOSTS').replace(" ","").split(',')

SECRET_KEY = 'django-insecure-default-key-for-development'
DEBUG = True
ALLOWED_HOSTS = ['localhost', '127.0.0.1']

# Kiểm tra xem đang chạy trong Docker hay không
def is_running_in_docker():
    try:
        with open('/proc/1/cgroup', 'rt') as f:
            return 'docker' in f.read()
    except:
        return False

# Biến môi trường cho Docker hoặc Local
IN_DOCKER = is_running_in_docker()

INSTALLED_APPS = [
    'django.contrib.admin',
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'django.contrib.sessions',
    'django.contrib.messages',
    'django.contrib.staticfiles',

    'django.contrib.humanize',
    'crispy_forms',
    'crispy_bootstrap5',

    'portfolio',
]

MIDDLEWARE = [
    'django.middleware.security.SecurityMiddleware',
    'whitenoise.middleware.WhiteNoiseMiddleware',
    'django.contrib.sessions.middleware.SessionMiddleware',
    'django.middleware.common.CommonMiddleware',
    'django.middleware.csrf.CsrfViewMiddleware',
    'django.contrib.auth.middleware.AuthenticationMiddleware',
    'django.contrib.messages.middleware.MessageMiddleware',
    'django.middleware.clickjacking.XFrameOptionsMiddleware',
]

ROOT_URLCONF = 'config.urls'

TEMPLATES = [
    {
        'BACKEND': 'django.template.backends.django.DjangoTemplates',
        'DIRS': [BASE_DIR / 'templates'],
        'APP_DIRS': True,
        'OPTIONS': {
            'context_processors': [
                'django.template.context_processors.debug',
                'django.template.context_processors.request',
                'django.contrib.auth.context_processors.auth',
                'django.contrib.messages.context_processors.messages',
            ],
        },
    },
]

WSGI_APPLICATION = 'config.wsgi.application'

# DATABASES = {
#     'default': {
#         'ENGINE': 'django.db.backends.sqlite3',
#         'NAME': BASE_DIR / 'db.sqlite3',
#     }
# }

DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.postgresql',
        'NAME': 'db_for_pm',
        'USER': 'airflow',
        'PASSWORD': 'admin123',
        'HOST': 'db',
        'PORT': '5432',
    }
}

# Ghi đè cấu hình từ biến môi trường nếu có
try:
    if config('DATABASE_NAME', default=None):
        DATABASES['default']['NAME'] = config('DATABASE_NAME')
    if config('DATABASE_USER', default=None):
        DATABASES['default']['USER'] = config('DATABASE_USER')
    if config('DATABASE_PASSWORD', default=None):
        DATABASES['default']['PASSWORD'] = config('DATABASE_PASSWORD')
    if config('DATABASE_HOST', default=None):
        DATABASES['default']['HOST'] = config('DATABASE_HOST')
    if config('DATABASE_PORT', default=None):
        DATABASES['default']['PORT'] = config('DATABASE_PORT')
except:
    pass

# Kiểm tra xem có thể kết nối tới 'db' không, nếu không thì dùng localhost
try:
    # Thử phân giải tên host 'db' thành địa chỉ IP
    if DATABASES['default']['HOST'] == 'db':
        socket.gethostbyname('db')
except socket.gaierror:
    # Không phân giải được, có thể đang chạy ngoài Docker
    print("Không thể kết nối tới host 'db'. Chuyển sang sử dụng PostgreSQL local...")
    DATABASES['default']['HOST'] = 'localhost'
    DATABASES['default']['USER'] = 'postgres'     # Sửa user thành 'postgres' cho môi trường local
    DATABASES['default']['PASSWORD'] = 'postgres' # Sửa password mặc định

print(f"Cấu hình Database: HOST={DATABASES['default']['HOST']}, NAME={DATABASES['default']['NAME']}, USER={DATABASES['default']['USER']}")

AUTH_PASSWORD_VALIDATORS = [
    {'NAME': 'django.contrib.auth.password_validation.UserAttributeSimilarityValidator',},
    {'NAME': 'django.contrib.auth.password_validation.MinimumLengthValidator',},
    {'NAME': 'django.contrib.auth.password_validation.CommonPasswordValidator',},
    {'NAME': 'django.contrib.auth.password_validation.NumericPasswordValidator',},
]

LANGUAGE_CODE = 'vi'
TIME_ZONE = 'Asia/Ho_Chi_Minh'
USE_I18N = True
USE_TZ = True

STATIC_URL = 'static/'
STATICFILES_DIRS = [BASE_DIR / 'static']
STATIC_ROOT = BASE_DIR / 'staticfiles'

# Cấu hình WhiteNoise cho môi trường production
# STORAGES = {
#     "staticfiles": {
#         "BACKEND": "whitenoise.storage.CompressedManifestStaticFilesStorage",
#     },
# }

MEDIA_URL = 'media/'
MEDIA_ROOT = BASE_DIR / 'media'

DEFAULT_AUTO_FIELD = 'django.db.models.BigAutoField'

CRISPY_ALLOWED_TEMPLATE_PACKS = "bootstrap5"
CRISPY_TEMPLATE_PACK = "bootstrap5"

LOGIN_URL = 'login'
LOGIN_REDIRECT_URL = 'dashboard'
LOGOUT_REDIRECT_URL = 'home'

AUTH_USER_MODEL = 'portfolio.User'

# Auth0 configuration
AUTH0_DOMAIN = 'dev-lrgbq7jwc8g46cow.us.auth0.com'
AUTH0_CLIENT_ID = 'r1u6ia6JhvGNoJA6bw52oylsNw40p2jm'
AUTH0_CLIENT_SECRET = 'lZ2pCeHzc9izvbfRF8QZpf1PU8dpqtVt0A2ReanWAUGenq8TsL6PNFXvjtrtsj4Z'
# Các URL chính xác sẽ được tạo động từ request

# Giúp tránh lỗi mismatching_state khi dùng Auth0 trên localhost
SESSION_COOKIE_SECURE = False
CSRF_COOKIE_SECURE = False
SESSION_COOKIE_SAMESITE = 'Lax'
CSRF_COOKIE_SAMESITE = 'Lax'

# Cấu hình logging
LOGGING = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'verbose': {
            'format': '{levelname} {asctime} - {name} - {message}',
            'style': '{',
        },
        'simple': {
            'format': '{levelname} {message}',
            'style': '{',
        },
    },
    'handlers': {
        'console': {
            'level': 'DEBUG',
            'class': 'logging.StreamHandler',
            'formatter': 'verbose',
        },
    },
    'loggers': {
        'django': {
            'handlers': ['console'],
            'level': 'INFO',
            'propagate': True,
        },
        'portfolio.kafka_service': {
            'handlers': ['console'],
            'level': 'DEBUG',  # Ghi log chi tiết cho module kafka_service
            'propagate': False,
        },
        'portfolio': {
            'handlers': ['console'],
            'level': 'INFO',
            'propagate': True,
        },
    },
}
