import asyncio
import logging
import re
import signal
import time
import tomllib
from pathlib import Path

from fritzconnection import FritzConnection

from config import get_first_config
from mqtt_handler import MqttHandler

with (Path(__file__).parent / "pyproject.toml").open("rb") as f:
    __version__ = tomllib.load(f)["project"]["version"]


class Fritz2Mqtt:
    def __init__(self) -> None:
        config: dict = get_first_config()
        if 'logging' in config:
            logging_level_name: str = config['logging'].upper()
            logging_level: int = logging.getLevelNamesMapping().get(logging_level_name, logging.NOTSET)
            if logging_level != logging.NOTSET:
                logging.getLogger().setLevel(logging_level)
            else:
                logging.warning('unknown logging level: %s.', logging_level)
        self.update_rate: int = config.get('update_rate', 60)
        self.mqtt_handler: MqttHandler = MqttHandler(config)
        self.fritz_address: str = config.get("fritz_address", "192.168.178.1")
        self.fritz_user: str = config.get("fritz_user", "monitoring")
        self.fritz_password: str = config.get("fritz_password", "")
        self.fritz_timeout: float = config.get("fritz_timeout", 10.0)
        self.fc: FritzConnection | None = None

    def get_cpu_temperature(self):
        if self.fc is None:
            return None
        temperatures = self.fc.get_cpu_temperatures()
        if not temperatures:
            return None
        temperature = temperatures[0]
        if temperature == 0:
            return None
        return float(temperature)

    def get_fiber_temperature(self):
        if self.fc is None:
            return None
        http = self.fc.http_interface
        sid = next(http._get_sid())
        response = self.fc.session.post(f"{http.router_url}/data.lua", data={"sid": sid, "page": "fiberFiber"})
        response.raise_for_status()
        data = response.json()
        properties = data.get("data", {}).get("sfpProperties", [])
        for prop in properties:
            title = str(prop.get("title", "")).lower()
            value = str(prop.get("val", ""))
            if "temper" not in title:
                continue
            if "°c" not in value.lower():
                continue
            match = re.search(r"-?\d+(?:[.,]\d+)?", value)
            if match:
                return float(match.group(0).replace(",", "."))
        return None

    def read_metrics(self) -> dict[str, float | None]:
        return {
            "cpu_temperature": self.get_cpu_temperature(),
            "fiber_temperature": self.get_fiber_temperature(),
        }

    async def loop(self) -> None:
        self.fc = await asyncio.to_thread(
            FritzConnection,
            address=self.fritz_address,
            user=self.fritz_user,
            password=self.fritz_password,
            timeout=self.fritz_timeout,
        )
        try:
            while True:
                start_time: float = time.perf_counter()
                metrics = await asyncio.to_thread(self.read_metrics)
                logging.debug('got metrics: %s', metrics)
                for metric_name, metric_value in metrics.items():
                    self.mqtt_handler.publish(metric_name, str(metric_value))
                time_taken: float = time.perf_counter() - start_time
                time_to_sleep: float = self.update_rate - time_taken
                logging.debug('looped in %.2fms, sleeping %.2fs.', time_taken * 1000, time_to_sleep)
                if time_to_sleep > 0:
                    await asyncio.sleep(time_to_sleep)
        except KeyboardInterrupt:
            await self.mqtt_handler.mqttc.disconnect()

    async def exit(self) -> None:
        await self.mqtt_handler.disconnect()


async def main():
    app = Fritz2Mqtt()
    loop = asyncio.get_running_loop()
    main_task = asyncio.current_task()

    def shutdown_handler():
        if not main_task.done():
            main_task.cancel()

    try:
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, shutdown_handler)
    except NotImplementedError:
        pass
    try:
        await app.loop()
    except asyncio.CancelledError:
        logging.info('exiting.')
    finally:
        await app.exit()
        logging.info('exited.')


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logging.getLogger('gmqtt').setLevel(logging.ERROR)
    logging.info('starting Fritz2Mqtt v%s.', __version__)
    asyncio.run(main())
