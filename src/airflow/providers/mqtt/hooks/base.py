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

from paho.mqtt import client as mqtt, enums as mqtt_enums
import socks
import random
import asyncio

import ssl

from airflow.providers.common.compat.sdk import BaseHook

class MqttBaseHook(BaseHook):

    conn_name_attr = "mqtt_conn_id"
    default_conn_name = "mqtt_default"
    conn_type = "mqtt"
    hook_name = "MQTT"

    def __init__(self, mqtt_config_id=default_conn_name, *args, **kwargs):
        super().__init__()
        self.mqtt_config_id = mqtt_config_id

    async def get_conn(self) -> mqtt.Client:

        config = await self.aget_connection(self.mqtt_config_id)
        
        loop = asyncio.get_running_loop()
        connected = loop.create_future()
        
        mqtt_version = mqtt.MQTTv311

        extra = config.extra_dejson

        mqtt_transport = extra["transport"] if extra is not None and "transport" in extra else "tcp"
        broker = config.host if config.host is not None else "127.0.0.1"
        port = config.port if config.port is not None else 1883
        username = config.login
        password = config.password
        if extra is not None:
            if "version" in extra:
                mqtt_version = {
                    "MQTTv5": mqtt.MQTTv5,
                    "MQTTv31": mqtt.MQTTv31,
                    "MQTTv311": mqtt.MQTTv311
                    }[extra["version"]]

        self.log.info("Connecting to %s:%d via %s", broker, port, mqtt_transport)
        client = mqtt.Client(client_id = f"airflow-{random.randint(0,1000)}",
                             callback_api_version=mqtt_enums.CallbackAPIVersion.VERSION2,
                             protocol = mqtt_version,
                             transport = mqtt_transport
                            )

        if extra is not None:
            if "proxy" in extra:
                proxy_config = extra["proxy"]
                proxy_type = socks.HTTP
                proxy_addr = None
                proxy_port = None
                proxy_rdns = True
                proxy_username= None
                proxy_password = None
                if "host" in proxy_config:
                    proxy_addr = proxy_config["host"]
                else:
                    raise Exception("No proxy host provided")
                if "port" in proxy_config:
                    proxy_port = proxy_config["port"]
                if "type" in proxy_config:
                    match proxy_config["type"]:
                        case "socks4":
                            proxy_type = socks.SOCKS4
                        case "socks5":
                            proxy_type = socks.SOCKS5
                        case "http":
                            proxy_type = socks.HTTP
                if "rdns" in proxy_config:
                    proxy_rdns = proxy_config["rdns"]
                if "username" in proxy_config:
                    proxy_username = proxy_config["username"]
                if "password" in proxy_config:
                    proxy_password = proxy_config["password"]
                self.log.debug("Setting proxy: type=%d, addr=%s, proxy_port=%d", proxy_type, proxy_addr, proxy_port)
                client.proxy_set(proxy_type=proxy_type, proxy_addr=proxy_addr,
                                 proxy_port=proxy_port, proxy_rdns=proxy_rdns,
                                 proxy_username=proxy_username, proxy_password=proxy_password)
        
        if config.schema == 'mqtts':

            ca_certs = None
            certfile = None
            keyfile = None
            keyfile_password = None
            cert_reqs = None
            ciphers = None

            if extra and "tls" in extra:
                extra_tls = extra["tls"]
                if "ca_certs" in extra_tls:
                    ca_certs = extra_tls["ca_certs"]
                if "certfile" in extra_tls:
                    certfile = extra_tls["certfile"]
                if "keyfile" in extra_tls:
                    keyfile = extra_tls["keyfile"]
                if "keyfile_password" in extra_tls:
                    keyfile_password = extra_tls["keyfile_password"]
                if "ciphers" in extra_tls:
                    ciphers = extra_tls["ciphers"]
                if "cert_reqs" in extra_tls:
                    cert_reqs = {
                        "none": ssl.CERT_NONE,
                        "optional": ssl.CERT_OPTIONAL,
                        "required": ssl.CERT_REQUIRED
                    }[extra_tls["cert_reqs"]]
            client.tls_set(ca_certs=ca_certs, certfile=certfile,
                           keyfile=keyfile, keyfile_password=keyfile_password,
                           cert_reqs=cert_reqs, ciphers=ciphers)
            if extra and "tls" in extra:
                extra_tls = extra["tls"]
                if "insecure" in extra_tls:
                    client.tls_insecure_set(extra_tls["insecure"])
        if extra and "debug" in extra:
            if extra["debug"]:
                client.enable_logger()
        
        def mqtt_on_connect(client, userdata, flags, rc, properties):
            if rc == 0:
                self.log.info("Connected to broker")
                if userdata:
                    userdata.get_loop().call_soon_threadsafe(userdata.set_result, client)
            else:
                self.log.error("Failed to connect to broker: %s", rc)
                if userdata:
                    userdata.get_loop().call_soon_threadsafe(userdata.set_exception, ValueError(rc))
            client.user_data_set(None)
        
        def mqtt_on_connect_fail(client, userdata):
            self.log.error("Failed to connect to broker")

        def mqtt_on_disconnect(client, userdata, flags, rc, properties):
            self.log.error("Disconnected from broker, rc = %s", rc)
            client.reconnect()
        
        if password != None:
            client.username_pw_set(username, password)
        
        client.on_connect = mqtt_on_connect
        client.on_connect_fail = mqtt_on_connect_fail
        client.on_disconnect = mqtt_on_disconnect
        client.user_data_set(connected)
        client.loop_start()
        client.connect_async(broker, port)
       
        self._client = client

        return await connected
    
    def disconnect(self) -> asyncio.Future[None]:
        
        loop = asyncio.get_running_loop()
        fut = loop.create_future()
        def mqtt_final_on_disconnect(client, userdata, flags, rc, properties):
            self.log.info("Disconnected mqtt client")
            fut.get_loop().call_soon_threadsafe(fut.set_result, None)
            client.loop_stop()
        
        self._client.on_disconnect = mqtt_final_on_disconnect
        self._client.disconnect()
        return fut

    def test_connection(self) -> tuple[bool, str]:
        return True, "Connection test dummy"

