import random
import uuid
from datetime import datetime, timedelta
from config import LATITUDE_INCREMENT, LONGITUDE_INCREMENT, KOLKATA_COORDINATES

start_time = datetime.now()
start_location = KOLKATA_COORDINATES.copy()

def get_next_time():
    global start_time
    start_time += timedelta(seconds=random.randint(30, 60))
    return start_time

def simulate_vehicle_movement():
    global start_location
    start_location['latitude'] += LATITUDE_INCREMENT
    start_location['longitude'] += LONGITUDE_INCREMENT
    start_location['latitude'] += random.uniform(-0.0005, 0.0005)
    start_location['longitude'] += random.uniform(-0.0005, 0.0005)
    return start_location

def generate_vehicle_data(vehicle_id):
    location = simulate_vehicle_movement()
    return {
        'id': uuid.uuid4(),
        'vehicle_id': vehicle_id,
        'timestamp': get_next_time().isoformat(),
        'location': (location['latitude'], location['longitude']),
        'speed': random.uniform(10, 80),
        'direction': random.choice(['North-East', 'South-West', 'East-West']),
        'make': 'Tesla',
        'model': 'Model S',
        'year': 2024,
        'fuelType': 'Electric'
    }

def generate_gps_data(vehicle_id, timestamp, vehicle_type='private'):
    return {
        'id': uuid.uuid4(),
        'vehicle_id': vehicle_id,
        'timestamp': timestamp,
        'speed': random.uniform(0, 80),
        'direction': random.choice(['North-East', 'South-West', 'East-West']),
        'vehicleType': vehicle_type
    }

def generate_weather_data(vehicle_id, timestamp, location):
    return {
        'id': uuid.uuid4(),
        'vehicle_id': vehicle_id,
        'timestamp': timestamp,
        'location': location,
        'temperature': random.uniform(15, 40),
        'weatherCondition': random.choice(['Sunny', 'Cloudy', 'Rain', 'Thunderstorm']),
        'windSpeed': random.uniform(0, 100),
        'humidity': random.randint(10, 90)
    }

def generate_traffic_camera_data(vehicle_id, timestamp, location, camera_id):
    return {
        'id': uuid.uuid4(),
        'vehicle_id': vehicle_id,
        'camera_id': camera_id,
        'timestamp': timestamp,
        'location': location,
        'snapshot': 'Base64EncodedString'
    }

def generate_emergency_incident_data(vehicle_id, timestamp, location):
    return {
        'id': uuid.uuid4(),
        'vehicle_id': vehicle_id,
        'incidentId': uuid.uuid4(),
        'type': random.choice(['Accident', 'Fire', 'Medical', 'Police', 'None']),
        'timestamp': timestamp,
        'location': location,
        'status': random.choice(['Active', 'Resolved']),
        'description': 'Description of the incident'
    }

def generate_advanced_telemetry(vehicle_id, timestamp, location):
    return {
        'id': uuid.uuid4(),
        'vehicle_id': vehicle_id,
        'timestamp': timestamp,
        'location': location,
        'batteryLevel': round(random.uniform(20, 100), 2),
        'tirePressure': [round(random.uniform(28, 35), 2) for _ in range(4)],
        'roadCondition': random.choice(['Dry', 'Wet', 'Icy', 'Under Construction']),
        'noiseLevel_dB': random.randint(40, 100),
        'collisionRisk': random.choice(['High', 'Medium', 'Low'])
    }

def generate_safety_data(vehicle_id, timestamp, location):
    return {
        'id': uuid.uuid4(),
        'vehicle_id': vehicle_id,
        'timestamp': timestamp,
        'location': location,
        'collisionDetected': random.choice([True, False]),
        'impactSeverity': random.choice(['Low', 'Medium', 'High']) if random.random() < 0.1 else 'None',
        'airbagDeployed': random.choice([True, False]) if random.random() < 0.05 else False,
        'blackBoxData': 'Sample black box data for analysis'
    }
