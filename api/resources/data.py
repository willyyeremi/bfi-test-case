from flask import Blueprint, jsonify
from flask_jwt_extended import jwt_required
from models import db
from models.processeddata import ProcessedData

data_bp = Blueprint('data', __name__)

@data_bp.route('/data', methods=['GET'])
@jwt_required()
def get_data():
    data = ProcessedData.query.all()
    result = [{'productmasterid': d.productmasterid, 'data': d.data, 'recommendation': d.recommendation} for d in data]
    return jsonify(result)
