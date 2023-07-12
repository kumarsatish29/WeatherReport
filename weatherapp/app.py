from flask import Flask, request, jsonify
from insertdata import WeatherRecord, YieldRecord
from insertdata import db

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://practifly:practifly@localhost:5433/test'
db.init_app(app)

# Define the routes
@app.route('/api/weather', methods=['GET'])
async def weather_endpoint():
    page = request.args.get('page', default=1, type=int)
    per_page = request.args.get('per_page', default=10, type=int)
    pagination = WeatherRecord.query.paginate(page=page, per_page=per_page, error_out=False)
    records = pagination.items
    data = []
    for record in records:
        data.append({
            'id': record.id,
            'date': record.date,
            'max_temperature': record.max_temperature,
            'min_temperature': record.min_temperature,
            'precipitation': record.precipitation
        })

    return jsonify({'data': data})


@app.route('/api/yield', methods=['GET'])
def yield_endpoint():
    page = request.args.get('page', default=1, type=int)
    per_page = request.args.get('per_page', default=10, type=int)
    pagination = YieldRecord.query.paginate(page=page, per_page=per_page, error_out=False)
    records = pagination.items
    data = []
    for record in records:
        data.append({
            'id': record.id,
            'year': record.year,
            'yield_value': record.yield_value
        })
    return jsonify({'data': data})


@app.route('/api/weather/stats', methods=['GET'])
def weather_stats_endpoint():
    query_type = request.args.get('query_type')

    if query_type == 'all':
        # Return all weather records
        weather_records = WeatherRecord.query.all()
    elif query_type == 'recent':
        # Return recent weather records
        weather_records = WeatherRecord.query.order_by(WeatherRecord.date.desc()).limit(10).all()
    else:
        # Filter weather records by date and station ID
        date = request.args.get('date')
        # Perform the query with the provided filters
        weather_records = WeatherRecord.query.filter_by(date=date).all()

    data = []
    for record in weather_records:
        data.append({
            'id': record.id,
            'date': record.date,
            'max_temperature': record.max_temperature,
            'min_temperature': record.min_temperature,
            'precipitation': record.precipitation
        })

    return jsonify({'data': data})


if __name__ == '__main__':
    app.run(debug=True)
