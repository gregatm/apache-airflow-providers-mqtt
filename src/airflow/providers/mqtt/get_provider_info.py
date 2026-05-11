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

def get_provider_info():
    return {
        "package-name": "apache-airflow-providers-mqtt",
        "name": "MQTT",
        "description": "MQTT Provider",
        "operators": [
            {
                "integration-name": "MQTT",
                "python-modules": [
                    "airflow.providers.mqtt.operators.subscribe"
                    "airflow.providers.mqtt.operators.publish"
                ]
            }
        ],
        "hooks": [
            {
                "integration-name": "MQTT",
                "python-modules": [
                    "airflow.providers.mqtt.hooks.base",
                    "airflow.providers.mqtt.hooks.subscribe",
                    "airflow.providers.mqtt.hooks.publish",
                ]
            }
        ],
        "triggers": [
            {
                "integration-name": "MQTT",
                "python-modules": [
                    "airflow.providers.mqtt.triggers.await_message"
                ],
            },
        ],
        "connection-types": [
            {
                "hook-class-name": "airflow.providers.mqtt.hooks.base.MqttBaseHook",
                "hook-name": "MQTT",
                "connection-type": "mqtt",
                "ui-field-behaviour": {
                    "relabeling": {"extra": "Config Dict"},
                },
            }
        ],
        "queues": ["airflow.providers.mqtt.queues.mqtt.MqttMessageQueueProvider"]
    }
