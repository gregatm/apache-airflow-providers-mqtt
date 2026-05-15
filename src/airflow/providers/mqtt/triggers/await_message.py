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

import asyncio
import logging
from collections.abc import Sequence
from functools import partial
from typing import Any

from airflow.providers.mqtt.hooks.subscribe import MqttSubscriberHook
from airflow.providers.mqtt.version_compat import AIRFLOW_V_3_0_PLUS
from airflow.providers.common.compat.module_loading import import_string
from airflow.triggers.base import TriggerEvent

if AIRFLOW_V_3_0_PLUS:
    from airflow.triggers.base import BaseEventTrigger
else:
    from airflow.triggers.base import BaseTrigger as BaseEventTrigger  # type: ignore

log = logging.getLogger(__name__)

class AwaitMessageTrigger(BaseEventTrigger):
    def __init__(
            self,
            topics: Sequence[str],
            apply_function: str | None = None,
            mqtt_conn_id: str = "mqtt_default",
            apply_function_args: Sequence[Any] | None = None,
            apply_function_kwargs: dict[Any, Any] | None = None,
    ) -> None:
        self.topics = topics
        self.mqtt_conn_id = mqtt_conn_id
        self.apply_function = apply_function
        self.apply_function_args = apply_function_args
        self.apply_function_kwargs = apply_function_kwargs if apply_function_kwargs is None or "Encoding.VAR" not in apply_function_kwargs else apply_function_kwargs["Encoding.VAR"]
        self._subscriber = None
    
    def serialize(self) -> tuple[str, dict[str, Any]]:
        return (
                "airflow.providers.mqtt.triggers.await_message.AwaitMessageTrigger",
                {
                    "topics": self.topics,
                    "mqtt_conn_id": self.mqtt_conn_id,
                    "apply_function": self.apply_function,
                    "apply_function_args": self.apply_function_args,
                    "apply_function_kwargs": self.apply_function_kwargs
                }
        )
    
    async def run(self):
        subscriber_hook = MqttSubscriberHook(topics=self.topics, mqtt_conn_id=self.mqtt_conn_id)
        subscription = await subscriber_hook.get_subscription();
        self._subscriber = subscriber_hook
        isRunning = True
        message_processing = None
        if self.apply_function:
            message_processing = import_string(self.apply_function)
            message_processing = partial(
                message_processing, *(self.apply_function_args or ()), **(self.apply_function_kwargs or {})
            )
        while isRunning:
            try:
                msg = await subscription.get()
                if message_processing:
                    msg = message_processing(msg)
                    if msg is not None:
                        yield TriggerEvent(msg)
                else:
                    yield TriggerEvent({
                        "topic": msg.topic,
                        "msg": msg.payload,
                        "retain": msg.retain,
                        "qos": msg.qos,
                        "timestamp": msg.timestamp,
                        "properties": msg.properties
                        })
            except asyncio.QueueShutDown:
                isRunning = False
                self.log.debug("Trigger ended due to queue shutting down")
    
    async def cleanup(self) -> None:
        await self._subscriber.cleanup()



