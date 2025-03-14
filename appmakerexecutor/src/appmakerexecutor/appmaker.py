"""
File that initializes an AppMakerExecutor.
"""

import os
import sys
import time
import logging
import string
import random
import multiprocessing
from dotenv import load_dotenv

from commlib.node import Node as CommlibNode
from commlib.transports.mqtt import ConnectionParameters as MQTTConnectionParameters
from commlib.transports.redis import ConnectionParameters as RedisConnectionParameters

from appmaker_executor import AppMakerExecutor # type: ignore # pylint: disable=import-error

def start_executor(uid, feedback_topic, conn_params, message):
    """
    Initializes and starts the AppMakerExecutor with the provided parameters.

    Args:
        uid (str): Unique identifier for the executor instance.
        feedback_topic (str): Topic for feedback communication.
        conn_params (dict): Connection parameters for the executor.
        message (str): Message containing the model to be loaded and executed.

    Returns:
        None
    """
    amexe = AppMakerExecutor( # pylint: disable=not-callable
        uid=uid,
        feedback_topic=feedback_topic,
        conn_params=conn_params,
    )
    amexe.load_model(message)
    amexe.execute()

class AppMaker:
    """
    A class that initializes an AppMakerExecutor.
    """
    def __init__(self, uid):
        self.uid = uid
        self.commlib_node = None
        self.local_commlib_node = None
        self.amexe = None
        self.conn_params = None
        self.logger = logging.getLogger(__name__)
        self.current_process = None
        self.streamsim_reset_rpc_client = None
        self.goaldsl_reset_rpc_client = None
        self.scores_publisher = None
        self.execution_uid = None

    def on_message(self, message):
        """
        Handles incoming messages.

        Args:
            message (dict): The message received.

        Returns:
            None
        """
        try:
            self.execution_uid = \
                ''.join(random.choices(string.ascii_uppercase + string.digits, k=16))
            if self.current_process is not None and self.current_process.is_alive():
                self.logger.warning("There is a process running, ignoring message")
                return

            self.logger.info("Received model")
            self.logger.info("Feedback on: %s", message['feedbackTopic'])

            self.current_process = multiprocessing.Process(
                target=start_executor,
                args=(self.uid, message['feedbackTopic'], self.conn_params, message)
            )
            self.current_process.start()
            self.current_process.join()
            self.current_process = None
            self.logger.info("All done")
        except Exception as e: # pylint: disable=broad-except
            self.logger.error("Error on message: %s", e)

    def on_internal_score(self, message):
        """
        Handles the internal score message and publishes it.

        Args:
            message (Any): The message containing the score information to be published.
        """
        message['execution_uid'] = self.execution_uid
        self.scores_publisher.publish(message)

    def on_message_stop(self, message): # pylint: disable=unused-argument
        """
        Handles the 'stop' message to terminate the current running process.

        Args:
            message: The message triggering the stop action. This argument is not used.

        Logs:
            - A warning if no process is currently running.
            - An info message when the process is successfully terminated.
            - An error message if an exception occurs during the termination process.
        """
        try:
            self.logger.critical("Received stop message: %s", message)
            if self.current_process is None:
                self.logger.warning("No process running")
                return
            self.logger.critical("Terminating process")
            self.current_process.terminate()
            self.current_process.join()
            self.current_process = None
            self.logger.critical("Process terminated")
            self.logger.critical("Stopping stream simulator")
            self.streamsim_reset_rpc_client.call({}, timeout=3)
            self.logger.critical("Stream simulator stopped")
            self.logger.critical("Stopping goal DSL")
            self.goaldsl_reset_rpc_client.call({}, timeout=3)
            self.logger.critical("Goal DSL stopped")
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

        # -------- MQTT interfaces --------
        self.conn_params = MQTTConnectionParameters(
            host=broker_host,
            port=broker_port,
            ssl=broker_ssl,
            username=broker_username,
            password=broker_password,
            reconnect_attempts=0,
        )

        self.commlib_node = CommlibNode(node_name=f'locsys.app_executor_node.{self.uid}',
            connection_params=self.conn_params,
            heartbeats=True,
            debug=True)

        self.commlib_node.create_subscriber(
            topic=f'appcreator.{self.uid}.deploy',
            on_message=self.on_message
        )
        self.logger.info("Subscribed to %s", f'appcreator.{self.uid}.deploy')

        self.commlib_node.create_subscriber(
            topic=f'appcreator.{self.uid}.stop',
            on_message=self.on_message_stop
        )
        self.logger.info("Subscribed to %s", f'appcreator.{self.uid}.stop')

        self.scores_publisher = self.commlib_node.create_publisher(
            topic='appcreator.scores',
        )

        self.commlib_node.run()

        # ---------- Redis interfaces ----------
        self.local_commlib_node = CommlibNode(node_name='locsys.app_executor_node_local',
            connection_params=RedisConnectionParameters(),
            heartbeats=False,
            debug=True)

        self.local_commlib_node.create_subscriber(
            topic='appcreator.local.stop',
            on_message=self.on_message_stop
        )

        self.streamsim_reset_rpc_client = self.local_commlib_node.create_rpc_client(
            rpc_name=f"streamsim.{self.uid}.reset",
        )

        self.goaldsl_reset_rpc_client = self.local_commlib_node.create_rpc_client(
            rpc_name=f"goaldsl.{self.uid}.killall_sync",
        )

        self.local_commlib_node.create_subscriber(
            topic='appcreator.scores.internal',
            on_message=self.on_internal_score
        )

        self.local_commlib_node.run()

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
            if not appmaker.commlib_node.health:
                try:
                    print("Connection lost, restarting nodes")
                    appmaker.commlib_node.stop()
                    del appmaker.commlib_node
                    appmaker.local_commlib_node.stop()
                    del appmaker.local_commlib_node
                    time.sleep(1)
                except Exception as e: # pylint: disable=broad-except
                    print("Error stopping nodes", e)
                appmaker.run()
    except KeyboardInterrupt:
        try:
            appmaker.commlib_node.stop()
        except Exception as e: # pylint: disable=broad-except
            print("Error: ", e)
        print("Bye!")
        exit(0)
