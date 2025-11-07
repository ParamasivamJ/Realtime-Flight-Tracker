import requests
import json
import time
import os
from kafka import KafkaProducer
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class OpenSkyProducer:
    def __init__(self):
        self.broker = os.getenv('KAFKA_BROKER', 'kafka:29092')
        self.backend_url = "http://flight-backend:8000"
        self.topic = 'flight-data-raw'
        self.opensky_url = "https://opensky-network.org/api/states/all"
        
        # Kafka producer for raw data
        self.kafka_producer = KafkaProducer(
            bootstrap_servers=[self.broker],
            value_serializer=lambda x: json.dumps(x).encode('utf-8'),
            batch_size=16384,
            linger_ms=10,
            retries=3
        )
        
    def fetch_flight_data(self):
        """Fetch data from OpenSky API"""
        try:
            response = requests.get(self.opensky_url, timeout=10)
            if response.status_code == 200:
                return response.json()
            else:
                logger.warning(f"API Error: {response.status_code}")
                return None
        except Exception as e:
            logger.error(f"Error fetching data: {e}")
            return None
    
    def process_flight_state(self, state):
        """Process a single flight state into structured data"""
        if state and len(state) >= 17:
            return {
                'icao24': state[0],
                'callsign': (state[1] or 'UNKNOWN').strip(),
                'origin_country': state[2],
                'longitude': state[5],
                'latitude': state[6],
                'altitude': state[7] or 0,
                'velocity': state[9] or 0,
                'heading': state[10] or 0,
                'vertical_rate': state[11] or 0,
                'geo_altitude': state[13],
                'timestamp': datetime.utcnow().isoformat()
            }
        return None
    
    def send_to_backend(self, flight_data):
        """Send processed flight data to backend API"""
        try:
            response = requests.post(
                f"{self.backend_url}/flights/ingest",
                json=[flight_data],
                timeout=10
            )
            if response.status_code == 200:
                result = response.json()
                logger.info(f"Sent flight {flight_data['callsign']} to backend - {result.get('inserted', 0)} inserted")
                return True
            else:
                logger.warning(f"Backend API error: {response.status_code} - {response.text}")
                return False
        except Exception as e:
            logger.error(f"Error sending to backend: {e}")
            return False
    
    def process_and_send(self):
        """Main processing loop"""
        data = self.fetch_flight_data()
        if data and 'states' in data:
            successful_sends = 0
            for state in data['states'][:50]:  # Limit for demo
                flight_data = self.process_flight_state(state)
                if flight_data:
                    # Send to Kafka (raw data)
                    self.kafka_producer.send(self.topic, flight_data)
                    
                    # Send to Backend (for Databricks)
                    if self.send_to_backend(flight_data):
                        successful_sends += 1
            
            self.kafka_producer.flush()
            logger.info(f"Processed {successful_sends} flights to backend and Kafka")
            return successful_sends
        return 0
    
    def run(self):
        """Main run loop"""
        logger.info("Starting OpenSky Flight Data Producer...")
        logger.info("Data flow: OpenSky → Backend → Databricks & Kafka")
        
        while True:
            try:
                processed_count = self.process_and_send()
                logger.info(f"Cycle completed. Processed {processed_count} flights")
            except Exception as e:
                logger.error(f"Error in main loop: {e}")
            
            # Respect OpenSky rate limits
            time.sleep(30)

if __name__ == "__main__":
    producer = OpenSkyProducer()
    producer.run()