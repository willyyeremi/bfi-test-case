from . import db

class ProcessedData(db.Model):
    __bind_key__ = 'data'
    __tablename__ = 'pricerecommendation'
    productmasterid = db.Column(db.Integer, primary_key=True)
    price = db.Column(db.Integer, nullable=False)
    date = db.Column(db.Date, nullable=False)