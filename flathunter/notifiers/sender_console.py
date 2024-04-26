"""Functions and classes related to sending Telegram messages"""
import json
import time
from typing import List, Dict, Optional

import requests

from flathunter.abstract_notifier import Notifier
from flathunter.abstract_processor import Processor
from flathunter.config import YamlConfig
from flathunter.exceptions import BotBlockedException
from flathunter.exceptions import UserDeactivatedException
from flathunter.logging import logger
from flathunter.utils.list import chunk_list


class SenderConsole(Processor, Notifier):
    """Expose processor that sends new exposes to the console"""

    def __init__(self, config: YamlConfig):
        self.config = config

    def notify(self, message: str):
        """
        Send messages to each of the receivers in receiver_ids
        :param message: a message that should be sent to users
        :return: None
        """
        logger.info(message)

    def process_expose(self, expose):
        """Send a message to a user describing the expose"""
        logger.info(expose)
        return expose

    def __get_text_message(self, expose: Dict) -> str:
        """
        Build text message based on the exposed data
        :param expose: dictionary
        :return: str
        """

        return self.config.message_format().format(
            crawler=expose.get('crawler', 'N/A'),
            title=expose.get('title', 'N/A'),
            rooms=expose.get('rooms', 'N/A'),
            size=expose.get('size', 'N/A'),
            price=expose.get('price', 'N/A'),
            url=expose.get('url', 'N/A'),
            address=expose.get('address', 'N/A'),
            durations=expose.get('durations', 'N/A')
        ).strip()
