import os
import datetime

from flask import Flask
from flask_sqlalchemy import SQLAlchemy

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://practifly:practifly@localhost:5433/test'
db = SQLAlchemy(app)

# Function to ingest weather data

class WeatherRecord(db.Model):
    __tablename__ = 'weather_stats'
    id = db.Column(db.Integer, primary_key=True)
    date = db.Column(db.Date)
    max_temperature = db.Column(db.Float)
    min_temperature = db.Column(db.Float)
    precipitation = db.Column(db.Float)


class YieldRecord(db.Model):
    __tablename__ = 'yield_record'
    id = db.Column(db.Integer, primary_key=True)
    year = db.Column(db.Integer)
    yield_value = db.Column(db.Float)

# def read_file_lines(file_path):
#     with open(file_path, "r") as file:
#         for line in file:
#             yield line
# def ingest_weather_data(file_path):
#     for line in read_file_lines(file_path):
#         parts = line.strip().split("\t")
#         date_str, max_temp_str, min_temp_str, precipitation_str = parts
#         # Check for duplicates
#
#         date = datetime.datetime.strptime(date_str, "%Y%m%d").date()
#         max_temp = float(max_temp_str)
#         min_temp = float(min_temp_str)
#         precipitation = float(precipitation_str)
#         weather_data = WeatherRecord(date=date, max_temperature=max_temp, min_temperature=min_temp, precipitation=precipitation)
#         db.session.add(weather_data)
#     db.session.commit()


# def ingest_data():
#     weather_data_directory = os.path.join(os.path.dirname(__file__),"../wx_data/")
#     weather_data_files = [f for f in os.listdir(weather_data_directory) if f.endswith(".txt")]
#     for file_name in weather_data_files:
#         file_path = os.path.join(weather_data_directory, file_name)
#         ingest_weather_data(file_path)
#
#     print(f"Data ingestion completed!")



# def process_yield_data():
#     start_time = datetime.datetime.now()
#     dir_path = os.path.join(os.path.dirname(__file__),"../yld_data/")
#     data_files = [f for f in os.listdir(dir_path) if f.endswith(".txt")]
#     for file_name in data_files:
#         file_path = os.path.join(dir_path, file_name)
#         for line in read_file_lines(file_path):
#             year_str, yield_value_str = line.strip().split('\t')
#             year = int(year_str)
#             yield_value = float(yield_value_str)
#
#             yield_record = YieldRecord(year=year, yield_value=yield_value)
#             db.session.add(yield_record)
#
#     db.session.commit()
#     end_time = datetime.datetime.now()
#     elapsed_time = end_time - start_time
#
#     # Log output
#     print(f"Data ingestion completed!")
#     print(f"Elapsed time: {elapsed_time.total_seconds()} seconds")
#     print(f"Weather records ingested: {total_weather_records}")



def ingest_weather_data(file_path):
    with open(file_path, "r") as file:
        lines = file.readlines()

    records_ingested = 0
    for line in lines:
        parts = line.strip().split("\t")
        date_str, max_temp_str, min_temp_str, precipitation_str = parts

        # Check for duplicates
        existing_record = WeatherRecord.query.filter_by(date=date_str).first()
        if existing_record:
            continue

        # Convert the data types
        date = datetime.datetime.strptime(date_str, "%Y%m%d").date()
        max_temp = float(max_temp_str)
        min_temp = float(min_temp_str)
        precipitation = float(precipitation_str)

        # Create a new WeatherRecord object
        record = WeatherRecord(date=date, max_temperature=max_temp, min_temperature=min_temp, precipitation=precipitation)

        # Add the record to the database
        db.session.add(record)
        records_ingested += 1

    # Commit the changes to the database
    db.session.commit()

    return records_ingested

def process_yield_data(file_path):
    with open(file_path, "r") as file:
        lines = file.readlines()

    records_ingested = 0
    for line in lines:
        year_str, yield_value_str = line.strip().split('\t')
        year = int(year_str)
        yield_value = float(yield_value_str)

        # Check for duplicates
        existing_record = YieldRecord.query.filter_by(year=year).first()
        if existing_record:
            continue

        # Create a new YieldRecord object
        record = YieldRecord(year=year, yield_value=yield_value)

        # Add the record to the database
        db.session.add(record)
        records_ingested += 1

    # Commit the changes to the database
    db.session.commit()

    return records_ingested


def insert_into_db():
    start_time = datetime.datetime.now()
    weather_data_directory = os.path.join(os.path.dirname(__file__), "../wx_data/")
    weather_data_files = [f for f in os.listdir(weather_data_directory) if f.endswith(".txt")]
    total_weather_records = 0
    for file_name in weather_data_files:
        file_path = os.path.join(weather_data_directory, file_name)
        weather_records_ingested = ingest_weather_data(file_path)
        total_weather_records += weather_records_ingested

    yield_data_directory = os.path.join(os.path.dirname(__file__), "../yld_data/")
    yield_data_files = [f for f in os.listdir(yield_data_directory) if f.endswith(".txt")]
    total_yield_records = 0
    for file_name in yield_data_files:
        file_path = os.path.join(yield_data_directory, file_name)
        yield_records_ingested = process_yield_data(file_path)
        total_yield_records += yield_records_ingested

    end_time = datetime.datetime.now()
    elapsed_time = end_time - start_time

    # Log output
    print(f"Data ingestion completed!")
    print(f"Elapsed time: {elapsed_time.total_seconds()} seconds")
    print(f"Weather records ingested: {total_weather_records}")
    print(f"Yield records ingested: {total_yield_records}")


if __name__ == '__main__':
    with app.app_context():
        db.create_all()
        insert_into_db()

