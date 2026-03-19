"""Mock sensor data producer — sends JSON events to Iggy."""

import asyncio
import json
import logging
import os
import random
import signal
from datetime import datetime, timezone

from apache_iggy import IggyClient, SendMessage

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("producer")

# Config
# Note: Use iggy:8090 for Docker internal network
IGGY_URL = os.environ.get("IGGY_URL", "iggy+tcp://iggy:iggy@iggy:8090")
STREAM = "quickstart"
TOPIC = "sensors"
INTERVAL = float(os.environ.get("PRODUCE_INTERVAL", "1.0"))

shutdown_event = asyncio.Event()

def handle_signal(sig, frame):
    log.info("Shutdown signal received")
    shutdown_event.set()

async def produce(client: IggyClient):
    log.info("Producing data to %s/%s every %ss...", STREAM, TOPIC, INTERVAL)
    
    while not shutdown_event.is_set():
        data = {
            "sensor_id": f"sensor_{random.randint(1, 5)}",
            "temperature": round(random.uniform(20.0, 35.0), 2),
            "humidity": round(random.uniform(30.0, 60.0), 2),
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

        payload = json.dumps(data)
        msg = SendMessage(payload)
        
        partition_id = random.randint(0, 2)
        await client.send_messages(STREAM, TOPIC, partition_id, [msg])

        try:
            await asyncio.wait_for(shutdown_event.wait(), timeout=INTERVAL)
        except asyncio.TimeoutError:
            pass

async def main():
    log.info("Connecting to Iggy: %s", IGGY_URL)
    
    # Simple retry loop for startup
    client = None
    for i in range(10):
        try:
            c = IggyClient.from_connection_string(IGGY_URL)
            await c.connect()
            await c.login_user("iggy", "iggy")
            client = c
            break
        except Exception as e:
            log.warning("Connection failed (%s), retrying in 2s...", e)
            await asyncio.sleep(2)
    
    if not client:
        log.error("Could not connect to Iggy")
        return

    # Idempotent setup
    try:
        await client.create_stream(STREAM)
        log.info("Created stream: %s", STREAM)
    except Exception:
        pass

    try:
        await client.create_topic(STREAM, TOPIC, partitions_count=3)
        log.info("Created topic: %s/%s", STREAM, TOPIC)
        await asyncio.sleep(1)
    except Exception:
        pass

    await produce(client)
    log.info("Producer stopped.")

if __name__ == "__main__":
    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)
    asyncio.run(main())
