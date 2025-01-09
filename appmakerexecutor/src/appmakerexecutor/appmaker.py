"""
File that initializes an AppMakerExecutor.
"""

import os
import sys
import time
import logging
from dotenv import load_dotenv

from commlib.node import Node as CommlibNode
from commlib.transports.mqtt import ConnectionParameters as MQTTConnectionParameters

from appmaker_executor import AppMakerExecutor # type: ignore # pylint: disable=import-error

class AppMaker:
    """
    A class that initializes an AppMakerExecutor.
    """
    def __init__(self, uid):
        self.uid = uid
        self.commlib_node = None
        self.amexe = None
        self.conn_params = None
        self.logger = logging.getLogger(__name__)

    def on_message(self, message):
        """
        Handles incoming messages.

        Args:
            message (dict): The message received.

        Returns:
            None
        """
        try:
            self.logger.info("Received model")
            self.logger.info("Feedback on: %s", message['feedbackTopic'])
            self.amexe = AppMakerExecutor( # pylint: disable=not-callable
                uid = self.uid,
                feedback_topic = message['feedbackTopic'],
                conn_params = self.conn_params,
            )
            self.amexe.load_model(message)
            self.amexe.execute()
            self.logger.info("All done")
        except Exception as e: # pylint: disable=broad-except
            self.logger.error("Error on message: %s", e)

    def run(self):
        """
        Runs the AppMaker.

        Returns:
            None
        """
        try:
            load_dotenv()
            broker_host = os.getenv('BROKER_HOST', 'broker.emqx.io')
            broker_port = int(os.getenv('BROKER_PORT', "8883"))
            broker_ssl = bool(os.getenv('BROKER_SSL', "True"))
            broker_username = os.getenv('BROKER_USERNAME', '')
            broker_password = os.getenv('BROKER_PASSWORD', '')
        except Exception as e: # pylint: disable=broad-except
            print("Error: ", e)
            exit(1)

        self.conn_params = MQTTConnectionParameters(
            host=broker_host,
            port=broker_port,
            ssl=broker_ssl,
            username=broker_username,
            password=broker_password,
        )

        self.commlib_node = CommlibNode(node_name='locsys.app_executor_node',
            connection_params=self.conn_params,
            heartbeats=False,
            debug=True)

        self.commlib_node.create_subscriber(
            topic=f'appcreator.{self.uid}.deploy',
            on_message=self.on_message
        )
        self.logger.warning("Subscribed to %s", f'appcreator.{self.uid}.deploy')

        self.commlib_node.run()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("You must provide a UID as argument:")
        print(">> python3 appmaker.py UID")
        exit(0)

    _uid = sys.argv[1]
    appmaker = AppMaker(_uid)
    appmaker.run()
    try:
        while True:
            time.sleep(0.1)
    except KeyboardInterrupt:
        try:
            appmaker.commlib_node.stop()
        except Exception as e: # pylint: disable=broad-except
            print("Error: ", e)
        print("Bye!")
        exit(0)
