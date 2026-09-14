import hashlib
import json
import logging
import os
import urllib.request

from gmqtt import Client as MQTTClient, Message

from background_tasks import run_in_background

USE_HOME_ASSISTANT_MQTT = os.getenv('USE_HOME_ASSISTANT_MQTT', '').lower() in {'1', 'true', 'yes'}


def get_home_assistant_mqtt() -> dict:
    token = os.environ['SUPERVISOR_TOKEN']
    request = urllib.request.Request(
        'http://supervisor/services/mqtt',
        headers={'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'},
        method='GET',
    )
    with urllib.request.urlopen(request, timeout=10) as response:
        result = json.load(response)
    if result.get('result') == 'error':
        raise RuntimeError(f'Failed to get Home Assistant MQTT service: {result.get("message", "unknown error")}')
    mqtt_config = result.get('data', result)
    if 'host' not in mqtt_config:
        raise RuntimeError(f'Invalid Home Assistant MQTT service response: {result}')
    return mqtt_config


class MqttHandler:
    def __init__(self, config: dict) -> None:
        if USE_HOME_ASSISTANT_MQTT:
            ha_config = get_home_assistant_mqtt()
            self.host: str = ha_config['host']
            self.port: int = ha_config['port']
            username: str = ha_config['username']
            password: str = ha_config['password']
        else:
            self.host: str = config['mqtt_server']
            self.port: int = config.get('mqtt_port', 1883)
            username: str = config['mqtt_username']
            password: str = config['mqtt_password']
        logging.info('mqtt: connecting to %s:%s as %s.', self.host, self.port, username)
        self.topic_prefix: str = config.get('mqtt_topic', 'fritz2mqtt').rstrip('/') + '/'

        client_id = hashlib.md5(f'fritz2mqtt-{self.host}{self.port}{self.topic_prefix}'.encode()).hexdigest()
        will_message: Message = Message(self.topic_prefix + 'available', 'offline', will_delay_interval=5, retain=True)
        self.mqttc: MQTTClient = MQTTClient(client_id=client_id, will_message=will_message)
        self.mqttc.on_connect = self.on_connect
        self.mqttc.on_disconnect = self.on_disconnect
        self.mqttc.set_auth_credentials(username, password)
        run_in_background(self.connect())

    def on_connect(self, client: MQTTClient, flags, rc, properties):
        self.publish('available', 'online', retain=True)
        logging.info('mqtt connected.')

    def publish(self, topic: str, payload: str | int | float, retain: bool = False) -> None:
        try:
            self.mqttc.publish(self.topic_prefix + topic, payload, retain=retain)
        except AttributeError:
            pass

    async def connect(self) -> bool:
        if self.mqttc.is_connected:
            return True
        try:
            await self.mqttc._connection.close()
        except AttributeError:
            pass
        except Exception as e:
            logging.warning(f'mqtt close: {self.host=}, {e=}')
        try:
            await self.mqttc.connect(self.host, self.port)
            return True
        except ConnectionRefusedError as e:
            logging.warning(f'mqtt: {self.host=}, {e=}')
        except Exception as e:
            logging.error(f'mqtt: {self.host=}, {e=}')
        return False

    async def disconnect(self):
        if self.mqttc.is_connected:
            await self.mqttc.disconnect(reason_code=4)

    @staticmethod
    def on_disconnect(packet, exc=None):
        logging.info('mqtt disconnected.')
