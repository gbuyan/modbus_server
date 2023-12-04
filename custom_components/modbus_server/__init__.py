"""Support for Apache Kafka."""
import asyncio

from pyModbusTCP.server import ModbusServer

import voluptuous as vol

from homeassistant.const import (
    CONF_IP_ADDRESS,
    CONF_PORT,
    EVENT_HOMEASSISTANT_STOP,
    EVENT_HOMEASSISTANT_START
)
from homeassistant.core import HomeAssistant
import homeassistant.helpers.config_validation as cv
from homeassistant.helpers.typing import ConfigType

import logging

_LOGGER: logging.Logger = logging.getLogger(__package__)


DOMAIN = "modbus_server"

CONFIG_SCHEMA = vol.Schema(
    {
        DOMAIN: vol.Schema(
            {
                vol.Required(CONF_IP_ADDRESS): cv.string,
                vol.Required(CONF_PORT): cv.port,
            }
        )
    },
    extra=vol.ALLOW_EXTRA,
)


async def async_setup(hass: HomeAssistant, config: ConfigType) -> bool:
    """Activate the Modbus Server integration."""

    conf = config[DOMAIN]

    _LOGGER.error(f"{DOMAIN} async_setup {conf[CONF_IP_ADDRESS]}:{conf[CONF_PORT]}")

    modbus_server = hass.data[DOMAIN] = ModbusServerManager(
        hass,
        conf[CONF_IP_ADDRESS],
        conf[CONF_PORT],
    )

    hass.bus.async_listen_once(EVENT_HOMEASSISTANT_STOP, modbus_server.shutdown)
    hass.bus.async_listen_once(EVENT_HOMEASSISTANT_START, modbus_server.start)

    return True


class DateTimeJSONEncoder(json.JSONEncoder):
    """Encode python objects.

    Additionally add encoding for datetime objects as isoformat.
    """

    def default(self, o):
        """Implement encoding logic."""
        if isinstance(o, datetime):
            return o.isoformat()
        return super().default(o)


class ModbusServerManager:
    """Define a manager to buffer events from modbus."""

    def __init__(
        self,
        hass,
        ip_address,
        port,
    ):
        """Initialize."""
        self._encoder = DateTimeJSONEncoder()
        self._hass = hass
        self._modbus_server = ModbusServer(ip_address, port, no_block=True)
        self._modbus_updated = ModbusUpdater(self._modbus_server)

    async def start(self):
        """Start the Kafka manager."""
        _LOGGER.error(f"{DOMAIN} Start ")

        await self._producer.start()
        self.__running = True
        self.__polling_task = asyncio.create_task(
            self._network_loop_retry(
          
                interval=0.1,
            ),
            name="Updater:start_polling:polling_task",
        )

    async def shutdown(self, _):
        """Shut the manager down."""
        _LOGGER.error(f"{DOMAIN} Stop ")
        await self._producer.stop()
        self.__running = False
        self.__polling_task.cancel()

    async def _network_loop_retry(self,  interval: float) -> None:
        state = [0]
        _LOGGER.error(f"{DOMAIN} start _network_loop_retry ")

        while self.__running:
            newState = self._modbus_server.data_bank.get_coils(0)
            if state != newState:
                state = newState
                self._hass.bus.fire("modbus_server_event", {'q1':newState[0]})
            await asyncio.sleep(interval)


class ModbusUpdater: 
    def __init__(self, modbus_server):
         self._modbus_server = modbus_server
         self._running = False;



        
        