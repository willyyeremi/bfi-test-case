import secrets

# Generate a secret key
secret_key = secrets.token_urlsafe(32)
print(f"SECRET_KEY: {secret_key}")

# Generate a JWT secret key
jwt_secret_key = secrets.token_urlsafe(32)
print(f"JWT_SECRET_KEY: {jwt_secret_key}")
