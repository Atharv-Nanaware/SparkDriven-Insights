import os

# Coordinates
KOLKATA_COORDINATES = {"latitude": 22.5726, "longitude": 88.3639}
BENGALURU_COORDINATES = {"latitude": 12.9716, "longitude": 77.5946}


LATITUDE_INCREMENT = (BENGALURU_COORDINATES['latitude'] - KOLKATA_COORDINATES['latitude']) / 100
LONGITUDE_INCREMENT = (BENGALURU_COORDINATES['longitude'] - KOLKATA_COORDINATES['longitude']) / 100

# Kafka Topics
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
VEHICLE_TOPIC = os.getenv('VEHICLE_TOPIC', 'vehicle_data')
GPS_TOPIC = os.getenv('GPS_TOPIC', 'gps_data')
TRAFFIC_TOPIC = os.getenv('TRAFFIC_TOPIC', 'traffic_data')
WEATHER_TOPIC = os.getenv('WEATHER_TOPIC', 'weather_data')
EMERGENCY_TOPIC = os.getenv('EMERGENCY_TOPIC', 'emergency_data')
ADVANCED_TELEMETRY_TOPIC = os.getenv('ADV_TELEMETRY_TOPIC', 'advanced_telemetry_data')
SAFETY_TOPIC = os.getenv('SAFETY_TOPIC', 'safety_data')


# AWS Configuration

configuration = {
    "AWS_ACCESS_KEY": "AWS_ACCESS_KEY",
    "AWS_SECRET_KEY": "AWS_SECRET_KEY",
}