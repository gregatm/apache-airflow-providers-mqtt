# apache-airflow-providers-mqtt

Provider for using MQTT with Apache Airflow

## Installation

```shell
pip install apache-airflow-providers-mqtt
```

## Usage

```python
from __future__ import annotations

from airflow.providers.common.messaging.triggers.msg_queue import MessageQueueTrigger
from airflow.sdk import Asset, AssetWatcher, dag, task

trigger = MessageQueueTrigger(scheme="mqtt", topics="topic/+")

asset = Asset("mqtt_queue_asset", watchers=[AssetWatcher(name="mqtt_watcher", trigger=trigger)])

@dag(schedule=[asset])
def mqtt_example():
    @task()
    def extract_message(triggering_asset_events=None):
        message = list(triggering_asset_events.values())[0][0].extra['payload']
        print(f"Received message: {message}")
        return message
    
    mqtt_msg = extract_message()

mqtt_example()
```
