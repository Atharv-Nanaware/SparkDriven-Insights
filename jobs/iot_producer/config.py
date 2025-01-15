import configparser
import os



parser = configparser.ConfigParser()
parser.read(os.path.join(os.path.dirname(__file__), '.../config/config.conf'))

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

aws_configuration = {
    "AWS_ACCESS_KEY": parser.get('aws', 'aws_secret_access_key'),
    "AWS_SECRET_KEY": parser.get('aws', 'aws_access_key_id'),
}

