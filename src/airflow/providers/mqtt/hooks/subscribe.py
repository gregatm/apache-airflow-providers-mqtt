# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from __future__ import annotations

from collections.abc import Sequence

import asyncio

from airflow.providers.mqtt.hooks.base import MqttBaseHook
from airflow.providers.common.compat.module_loading import import_string

from paho.mqtt import SubscribeOptions

class MqttSubscriberHook(MqttBaseHook):
    
    def __init__(self, topics: str | tuple[str, int] | tuple[str, SubscribeOptions] | list[tuple[str, int]] | list[tuple[str, SubscribeOptions]], mqtt_conn_id=MqttBaseHook.default_conn_name) -> None:
        super().__init__(mqtt_conn_id=mqtt_conn_id)
        self.topics = topics
        self.queue = asyncio.Queue()

    async def get_subscription(self):
        
        loop = asyncio.get_event_loop()
        subscribed = loop.create_future()

        def on_message(client, userdata, msg):
            self.log.debug("Received message: %s", msg)
            self.queue.put_nowait(msg)
        
        def on_subscribe(client, userdata, mid, rc_list, properties):
            failed = False
            failed_topics = []
            for topic, grant in zip(self.topics, rc_list):
                if grant.is_failure:
                    self.log.error("Failed to subscribe to topic %s: %s", topic, grant)
                    failed = True
                    failed_topics.append(topic)
            if failed:
                subscribed.set_exception(ValueError("Failed to subscribe to topics", failed_topics))
            else:
                self.log.info("Successfully subscribed to topics: %s", rc_list)
                subscribed.set_result(None)

        self.client = self.get_conn
        self.client.on_subscribe = on_subscribe

        self.log.debug("Subscribe to topics: %s", self.topics)
        self.client.subscribe(self.topics)
        
        self.client.on_message = on_message

        await subscribed

        return self.queue
    
    async def cleanup(self):
        self.queue.shutdown()
        await super().disconnect()

