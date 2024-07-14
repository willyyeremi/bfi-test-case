import os

class Config:
    SQLALCHEMY_BINDS = {
        'data': os.getenv('DATABASE_URL', 'postgresql://admin:admin@localhost:5432/online_shop')
        ,'user': os.getenv('USERS_DATABASE_URL', 'postgresql://admin:admin@localhost:5432/api_user_access')
    }
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    SECRET_KEY = os.getenv('SECRET_KEY', 'KgADrbHr5eaCY6Hle46nd3Fck9yC1wl7qMUlFqmeMm0')
    JWT_SECRET_KEY = os.getenv('JWT_SECRET_KEY', 'WM3L52OnS3DCFpl6GwGuxAwK8yCHaa9OXsx_vEG8uG4') 