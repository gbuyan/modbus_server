"""Support for Apache Kafka."""
import asyncio

from pyModbusTCP.server import ModbusServer

import voluptuous as vol

from homeassistant.const import (
    CONF_HOST,
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
CONF_MSG_WAIT = "message_wait_seconds"


CONFIG_SCHEMA = vol.Schema(
    {
        DOMAIN: vol.Schema(
            {
                vol.Required(CONF_HOST): cv.string,
                vol.Required(CONF_PORT): cv.port,
                vol.Required(CONF_MSG_WAIT): cv.positive_float,

            }
        )
    },
    extra=vol.ALLOW_EXTRA,
)


async def async_setup(hass: HomeAssistant, config: ConfigType) -> bool:
    """Activate the Modbus Server integration."""

    conf:dict[str, Any] = config[DOMAIN]

    _LOGGER.error(f"{DOMAIN} async_setup {conf[CONF_HOST]}:{conf[CONF_PORT]}")

    modbus_server = hass.data[DOMAIN] = ModbusServerManager(
        hass,
        conf[CONF_HOST],
        conf[CONF_PORT],
        conf.get(CONF_MSG_WAIT, 0.1)
    )

    hass.bus.async_listen_once(EVENT_HOMEASSISTANT_STOP, modbus_server.shutdown)
    hass.bus.async_listen_once(EVENT_HOMEASSISTANT_START, modbus_server.start)

    return True

class ModbusServerManager:
    """Define a manager to buffer events from modbus."""

    def __init__(
        self,
        hass,
        host,
        port,
        msg_wait
    ):
        """Initialize."""
        self._hass = hass
        self._modbus_server = ModbusServer(host, port, no_block=True)
        self.__running = False
        self.__msg_wait = msg_wait

    async def start(self, event=None):
        """Start the Kafka manager."""
        _LOGGER.error(f"{DOMAIN} Start ")

        self._modbus_server.start()
        self.__running = True

        self.__polling_task = asyncio.create_task(
            self._handle_updates(
                interval=self.__msg_wait,
            ),
            name="ModebusUpdater:start_polling:polling_task",
        )

    async def shutdown(self, event=None):
        """Shut the manager down."""
        _LOGGER.error(f"{DOMAIN} Stop ")

        self.__running = False
        self.__polling_task.cancel()
        self._modbus_server.stop()

    async def _handle_updates(self,  interval: float) -> None:
        state = [0] * 32
        _LOGGER.error(f"{DOMAIN} start _network_loop_retry {self.__running}")

        while self.__running:
            newState = self._modbus_server.data_bank.get_coils(0, number=32)
            for index, item in enumerate(newState):
                if state[index] != item:
                    _LOGGER.error(f"{DOMAIN} fire event {index}:{item}")
                    self._hass.bus.fire("modbus_server_event", {('q'+index): item})
            state = newState.copy()
            await asyncio.sleep(interval)



        
        