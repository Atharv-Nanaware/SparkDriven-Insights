from confluent_kafka import SerializingProducer
from vehicle_simulation import simulate_journey
from config import KAFKA_BOOTSTRAP_SERVERS

if __name__ == '__main__':
    producer_config = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'error_cb': lambda err: print(f'Kafka Error: {err}')
    }
    producer = SerializingProducer(producer_config)

    try:
        simulate_journey(producer, 'Vehicle-Kolkata-Bengaluru-001')
    except KeyboardInterrupt:
        print('Simulation ended by the user.')
    except Exception as e:
        print(f'An unexpected error occurred: {e}')
