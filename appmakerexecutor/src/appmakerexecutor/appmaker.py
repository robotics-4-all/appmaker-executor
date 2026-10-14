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

from appmaker_executor import AppMakerExecutor  # type: ignore # pylint: disable=import-error
import config as CONFIG  # type: ignore # pylint: disable=import-error

logging.basicConfig(format="%(asctime)s - %(message)s")
if CONFIG.ZERO_LOGS:
    logging.disable()
else:
    logging.getLogger().setLevel(CONFIG.LOG_LEVEL)


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
    amexe = AppMakerExecutor(  # pylint: disable=not-callable
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

    MQTT_CONNECT_ATTEMPTS = 20
    MQTT_CONNECT_DELAY = 1.0
    MQTT_CONNECT_DELAY_MAX = 10.0

    def __init__(self, uid):
        self.uid = uid
        self.commlib_node = None
        self.local_commlib_node = None
        self.amexe = None
        self.conn_params = None
        self.current_process = None
        self.streamsim_reset_rpc_client = None
        self.goaldsl_reset_rpc_client = None
        self.scores_publisher = None
        self.execution_uid = None
        self.goaldsl_scores_publisher = None

        self._logger = logging.getLogger(__name__)

    @property
    def logger(self):
        return self._logger

    def _make_commlib_node(self):
        return CommlibNode(
            node_name=f"locsys.app_executor_node.{self.uid}",
            connection_params=self.conn_params,
            heartbeats=True,
            debug=True,
        )

    def create_mqtt_interface(self, description, create):
        """
        Creates an MQTT interface, retrying while the connection is dropped.

        The broker silently discards a large share of TCP connection attempts,
        and commlib raises on the first failure, so a single lost SYN must not
        take the whole executor down.

        Args:
            description (str): What is being created, used for logging.
            create (Callable): Factory returning the commlib interface.

        Returns:
            The commlib subscriber or publisher.

        Raises:
            ConnectionError: If every attempt failed.
        """
        delay = self.MQTT_CONNECT_DELAY
        for attempt in range(1, self.MQTT_CONNECT_ATTEMPTS + 1):
            try:
                return create()
            except ConnectionError:
                if attempt == self.MQTT_CONNECT_ATTEMPTS:
                    self.logger.critical(
                        "Could not create %s after %s attempts", description, attempt
                    )
                    raise
                self.logger.warning(
                    "Connection dropped while creating %s (attempt %s/%s), "
                    "retrying in %.1fs",
                    description,
                    attempt,
                    self.MQTT_CONNECT_ATTEMPTS,
                    delay,
                )
                time.sleep(delay)
                delay = min(delay * 2, self.MQTT_CONNECT_DELAY_MAX)
                try:
                    self.commlib_node.stop()
                except Exception:  # pylint: disable=broad-except
                    pass
                self.commlib_node = self._make_commlib_node()
        raise ConnectionError(description)

    def on_message(self, message):
        """
        Handles incoming messages.

        Args:
            message (dict): The message received.

        Returns:
            None
        """
        try:
            print(f"I RECEIVED{message}")
            self.execution_uid = "".join(
                random.choices(string.ascii_uppercase + string.digits, k=16)
            )
            if self.current_process is not None and self.current_process.is_alive():
                self.logger.warning("There is a process running, ignoring message")
                return

            self.logger.critical("Received model for execution")
            self.logger.info("Feedback on: %s", message["feedbackTopic"])

            self.current_process = multiprocessing.Process(
                target=start_executor,
                args=(self.uid, message["feedbackTopic"], self.conn_params, message),
            )
            self.current_process.start()
            self.current_process.join()
            self.current_process = None
            self.logger.critical("Execution with id=%s finished", self.execution_uid)
        except Exception as e:  # pylint: disable=broad-except
            self.logger.error("Error on message: %s", e)

    def on_internal_score(self, message):
        """
        Handles the internal score message and publishes it.

        Args:
            message (Any): The message containing the score information to be published.
        """
        message["execution_uid"] = self.execution_uid
        self.scores_publisher.publish(message)

        self.goaldsl_scores_publisher.publish(
            {
                "type": "goaldsl_update",
                "data": message,
            }
        )

    def on_message_stop(self, message):  # pylint: disable=unused-argument
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
        except Exception as e:  # pylint: disable=broad-except
            self.logger.error("Error on message: %s", e)

    def mqtt_alive(self):
        """
        Reports whether every MQTT endpoint transport is still connected.

        commlib raises ConnectionError inside the paho network thread on any
        disconnect, which kills the thread while the node state stays RUNNING
        forever. The old watchdog only looked at the node state, so a dropped
        broker connection left the executor deaf without ever restarting.
        """
        try:
            node = self.commlib_node
            endpoints = list(getattr(node, "_subscribers", [])
                             + getattr(node, "_publishers", []))
        except Exception:  # pylint: disable=broad-except
            return False
        if not endpoints:
            return False
        for endpoint in endpoints:
            transport = getattr(endpoint, "_transport", None)
            if transport is None or not getattr(transport, "is_connected", False):
                return False
        return True

    def run(self):
        """
        Runs the AppMaker.

        Returns:
            None
        """
        try:
            load_dotenv()
            broker_host = os.getenv("BROKER_HOST", "broker.emqx.io")
            broker_port = int(os.getenv("BROKER_PORT", "8883"))
            broker_ssl = (
                True if os.getenv("BROKER_SSL", "True").lower() == "true" else False
            )
            broker_username = os.getenv("BROKER_USERNAME", "")
            broker_password = os.getenv("BROKER_PASSWORD", "")
        except Exception as e:  # pylint: disable=broad-except
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
            tls_insecure=True,
        )

        self.commlib_node = self._make_commlib_node()
        print("Connected to broker")

        deploy_topic = f"appcreator/{self.uid}/deploy"
        self.create_mqtt_interface(
            f"subscriber for {deploy_topic}",
            lambda: self.commlib_node.create_subscriber(
                topic=deploy_topic, on_message=self.on_message
            ),
        )
        self.logger.info("Subscribed to %s", deploy_topic)

        stop_topic = f"appcreator/{self.uid}/stop"
        self.create_mqtt_interface(
            f"subscriber for {stop_topic}",
            lambda: self.commlib_node.create_subscriber(
                topic=stop_topic, on_message=self.on_message_stop
            ),
        )
        self.logger.info("Subscribed to %s", stop_topic)

        self.scores_publisher = self.create_mqtt_interface(
            "publisher for appcreator.scores",
            lambda: self.commlib_node.create_publisher(
                topic="appcreator.scores",
            ),
        )
        goaldsl_topic = f"streamsim.{self.uid}.notify"
        self.goaldsl_scores_publisher = self.create_mqtt_interface(
            f"publisher for {goaldsl_topic}",
            lambda: self.commlib_node.create_publisher(
                topic=goaldsl_topic,
            ),
        )

        self.commlib_node.run()

        # ---------- Redis interfaces ----------
        self.local_commlib_node = CommlibNode(
            node_name="locsys.app_executor_node_local",
            connection_params=RedisConnectionParameters(
                host=CONFIG.REDIS_HOST,
                port=CONFIG.REDIS_PORT,
                username=CONFIG.REDIS_USERNAME,
                password=CONFIG.REDIS_PASSWORD,
                db=CONFIG.REDIS_DB,
            ),
            heartbeats=False,
            debug=True,
        )

        self.local_commlib_node.create_subscriber(
            topic="appcreator.local.stop", on_message=self.on_message_stop
        )

        self.streamsim_reset_rpc_client = self.local_commlib_node.create_rpc_client(
            rpc_name=f"streamsim.{self.uid}.reset",
        )

        self.goaldsl_reset_rpc_client = self.local_commlib_node.create_rpc_client(
            rpc_name=f"goaldsl.{self.uid}.killall_sync",
        )

        self.local_commlib_node.create_subscriber(
            topic="appcreator.scores.internal", on_message=self.on_internal_score
        )

        self.local_commlib_node.run()


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("You must provide a UID as argument:")
        print(">> python3 appmaker.py UID")
        exit(0)

    _uid = sys.argv[1]
    _hz = 0.1
    _restart_after_dead_secs = 15.0
    appmaker = AppMaker(_uid)
    appmaker.run()
    try:
        dead_since = None
        while True:
            time.sleep(_hz)
            # if not appmaker.commlib_node.health:
            try:
                node_running = appmaker.commlib_node.state.name == "RUNNING"
            except Exception:  # pylint: disable=broad-except
                node_running = False
            if not node_running:
                dead_since = None
            elif appmaker.mqtt_alive():
                dead_since = None
                continue
            elif dead_since is None:
                # Grace period: CONNACK may still be in flight right after
                # startup, so only restart after sustained transport death.
                dead_since = time.monotonic()
                print("MQTT connection lost, waiting for recovery before restarting nodes")
                continue
            elif time.monotonic() - dead_since < _restart_after_dead_secs:
                continue
            try:
                print("MQTT connection lost, restarting nodes")
                appmaker.commlib_node.stop()
                del appmaker.commlib_node
                appmaker.local_commlib_node.stop()
                del appmaker.local_commlib_node
                time.sleep(1)
            except Exception as e:  # pylint: disable=broad-except
                print("Error stopping nodes", e)
            appmaker.run()
            dead_since = None
    except KeyboardInterrupt:
        try:
            appmaker.commlib_node.stop()
        except Exception as e:  # pylint: disable=broad-except
            print("Error: ", e)
        print("Bye!")
        exit(0)
