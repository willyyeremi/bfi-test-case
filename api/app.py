from flask import Flask
from flask_jwt_extended import JWTManager
from config import Config
from models import db
from resources.auth import auth_bp
from resources.data import data_bp

app = Flask(__name__)
app.config.from_object(Config)

db.init_app(app)
JWTManager(app)

app.register_blueprint(auth_bp, url_prefix='/auth')
app.register_blueprint(data_bp, url_prefix='/api')

if __name__ == '__main__':
    app.run(debug=True)