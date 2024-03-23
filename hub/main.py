import logging
from typing import List
import time
from fastapi import FastAPI
from redis import Redis
import json
from datetime import datetime
import paho.mqtt.client as mqtt

from app.adapters.store_api_adapter import StoreApiAdapter
from app.entities.processed_agent_data import ProcessedAgentData
from config import (
    STORE_API_BASE_URL,
    REDIS_HOST,
    REDIS_PORT,
    BATCH_SIZE,
    MQTT_TOPIC,
    MQTT_BROKER_HOST,
    MQTT_BROKER_PORT,
)


class MQTTClient:
    def __init__(self, mqtt_broker_host: str, mqtt_broker_port: int, mqtt_topic: str):
        self.client = mqtt.Client()
        self.mqtt_broker_host = mqtt_broker_host
        self.mqtt_broker_port = mqtt_broker_port
        self.mqtt_topic = mqtt_topic

        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message

        # Create instances of Redis and StoreApiAdapter
        self.redis_client = Redis(host=REDIS_HOST, port=REDIS_PORT)
        self.store_adapter = StoreApiAdapter(api_base_url=STORE_API_BASE_URL)

    def on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            logging.info("Connected to MQTT broker")
            client.subscribe(self.mqtt_topic)
        else:
            logging.info(f"Failed to connect to MQTT broker with code: {rc}")

    def on_message(self, client, userdata, msg):
        try:
            payload: str = msg.payload.decode("utf-8")
            logging.info(f"mqtt message: {payload}")
            processed_agent_data = ProcessedAgentData.model_validate_json(
                payload, strict=True
            )
            # Save processed data to the database
            self.redis_client.lpush(
                "processed_agent_data", processed_agent_data.model_dump_json()
            )
            if self.redis_client.llen("processed_agent_data") >= BATCH_SIZE:
                processed_agent_data_batch: List[ProcessedAgentData] = []
                for _ in range(BATCH_SIZE):
                    processed_agent_data = ProcessedAgentData.model_validate_json(
                        self.redis_client.lpop("processed_agent_data")
                    )
                    processed_agent_data_batch.append(processed_agent_data)
                self.store_adapter.save_data(
                    processed_agent_data_batch=processed_agent_data_batch
                )
                logging.info(f"Saved {BATCH_SIZE} messages to db")
        except Exception as e:
            logging.info(f"Error processing MQTT message: {e}")

    def connect(self):
        self.client.connect(self.mqtt_broker_host, self.mqtt_broker_port)

    def start(self):
        self.client.loop_start()

'''
    def emulate_requests(self):
        processed_data = {
            "road_state": "flat",
            "agent_data": {
                "user_id" : 1,
                "accelerometer": {"x": -17, "y": 4, "z": 16516},
                "gps": {"latitude": 50.450386085935094, "longitude": 30.524547100067142},
                "timestamp": "2024-03-21T20:54:00.236457",
            },
        }
        json_string = json.dumps(processed_data)
        while True:
            # Publish the encoded payload
            self.client.publish(self.mqtt_topic, payload=json_string)
            time.sleep(5)
'''

# Initialize FastAPI app
app = FastAPI()

# Configure logging settings
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [%(levelname)s] [%(module)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("app.log"),
    ],
)

# Initialize and start MQTT client
mqtt_client = MQTTClient(
    mqtt_broker_host=MQTT_BROKER_HOST,
    mqtt_broker_port=MQTT_BROKER_PORT,
    mqtt_topic=MQTT_TOPIC,
)
mqtt_client.connect()
mqtt_client.start()

# Start MQTT request emulator
#mqtt_client.emulate_requests()