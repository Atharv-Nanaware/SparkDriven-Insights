import time
import json
from confluent_kafka import SerializingProducer


from data_generators import (
    generate_vehicle_data,
    generate_gps_data,
    generate_weather_data,
    generate_traffic_camera_data,
    generate_emergency_incident_data,
    generate_advanced_telemetry,
    generate_safety_data
)
from utils import json_serializer, delivery_report
from config import (
    KAFKA_BOOTSTRAP_SERVERS,
    VEHICLE_TOPIC,
    GPS_TOPIC,
    TRAFFIC_TOPIC,
    WEATHER_TOPIC,
    EMERGENCY_TOPIC,
    ADVANCED_TELEMETRY_TOPIC,
    SAFETY_TOPIC
)

def produce_data_to_kafka(producer, topic, data):
    producer.produce(
        topic,
        key=str(data['id']),
        value=json.dumps(data, default=json_serializer).encode('utf-8'),
        on_delivery=delivery_report
    )
    producer.flush()

def simulate_journey(producer, vehicle_id):
    while True:
        vehicle_data = generate_vehicle_data(vehicle_id)
        gps_data = generate_gps_data(vehicle_id, vehicle_data['timestamp'])
        weather_data = generate_weather_data(vehicle_id, vehicle_data['timestamp'], vehicle_data['location'])
        traffic_camera_data = generate_traffic_camera_data(vehicle_id, vehicle_data['timestamp'], vehicle_data['location'], "Intelligent-Cam-001")
        emergency_data = generate_emergency_incident_data(vehicle_id, vehicle_data['timestamp'], vehicle_data['location'])
        advanced_telemetry = generate_advanced_telemetry(vehicle_id, vehicle_data['timestamp'], vehicle_data['location'])
        safety_data = generate_safety_data(vehicle_id, vehicle_data['timestamp'], vehicle_data['location'])

        produce_data_to_kafka(producer, VEHICLE_TOPIC, vehicle_data)
        produce_data_to_kafka(producer, GPS_TOPIC, gps_data)
        produce_data_to_kafka(producer, WEATHER_TOPIC, weather_data)
        produce_data_to_kafka(producer, TRAFFIC_TOPIC, traffic_camera_data)
        produce_data_to_kafka(producer, EMERGENCY_TOPIC, emergency_data)
        produce_data_to_kafka(producer, ADVANCED_TELEMETRY_TOPIC, advanced_telemetry)
        produce_data_to_kafka(producer, SAFETY_TOPIC, safety_data)

        time.sleep(5)
