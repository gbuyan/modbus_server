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

class ModbusServerManager:
    """Define a manager to buffer events from modbus."""

    def __init__(
        self,
        hass,
        ip_address,
        port,
    ):
        """Initialize."""
        self._hass = hass
        self._modbus_server = ModbusServer(ip_address, port, no_block=True)
        self.__running = False

    async def start(self, event=None):
        """Start the Kafka manager."""
        _LOGGER.error(f"{DOMAIN} Start ")

        self.__running = True
        self._modbus_server.start()
        self.__polling_task = asyncio.create_task(
            self._network_loop_retry(
                interval=0.1,
            ),
            name="Updater:start_polling:polling_task",
        )

    async def shutdown(self, event=None):
        """Shut the manager down."""
        _LOGGER.error(f"{DOMAIN} Stop ")

        self.__running = False
        self.__polling_task.cancel()
        self._modbus_server.stop()

    async def _network_loop_retry(self,  interval: float) -> None:
        state = [0]
        _LOGGER.error(f"{DOMAIN} start _network_loop_retry {self.__running}")

        while self.__running:
            newState = self._modbus_server.data_bank.get_coils(0)
            if state != newState:
                state = newState
                _LOGGER.error(f"{DOMAIN} fire event {newState[0]}")
                self._hass.bus.fire("modbus_server_event", {'q1':newState[0]})
            await asyncio.sleep(interval)



        
        