"""Publishes sample barcode print jobs to a crucible-label-printer over MQTT."""
import json
import os
import time
import uuid

import paho.mqtt.client as mqtt
from dotenv import load_dotenv

load_dotenv()

MQTT_BROKER = os.environ.get("MQTT_BROKER", "mqtt.mfdata.org")
MQTT_PORT = int(os.environ.get("MQTT_PORT", "8883"))
MQTT_USERNAME = os.environ.get("MQTT_USERNAME", "crucible-printers")
MQTT_PASSWORD = os.environ.get("MQTT_PASSWORD")
MQTT_CA_CERTS = os.environ.get("MQTT_CA_CERTS")  # optional path to CA bundle


def send_print_job(printer_id: str, mfid: str, name: str) -> str:
    """Publish a print job to crucible-printer/<printer_id>/print. Returns the job_id."""
    if not printer_id:
        raise RuntimeError("printer_id is required")
    if not MQTT_PASSWORD:
        raise RuntimeError("MQTT_PASSWORD must be set (see env.sample)")

    payload = {
        "job_id": str(uuid.uuid4()),
        "mfid": mfid,
        "name": name,
        "ts": time.time(),
    }
    topic = f"crucible-printer/{printer_id}/print"

    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=f"upload-ui-{payload['job_id']}")
    client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
    client.tls_set(ca_certs=MQTT_CA_CERTS)
    client.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)
    client.loop_start()
    try:
        info = client.publish(topic, json.dumps(payload), qos=1)
        info.wait_for_publish(timeout=10)
    finally:
        client.loop_stop()
        client.disconnect()

    return payload["job_id"]
