from flask_sqlalchemy import SQLAlchemy

db = SQLAlchemy()

# Importing models to bind to the db instance
from .user import User
from .processeddata import ProcessedData
