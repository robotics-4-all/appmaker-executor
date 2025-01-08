"""
This module provides a StorageHandler class for managing key-value storage.
"""

import re
import time
import logging
import copy

from commlib.node import Node as CommlibNode
from commlib.transports.redis import ConnectionParameters as RedisConnectionParameters

class StorageHandler:
    """
    A class for managing key-value storage.
    """
    def __init__(self):
        self.storage = {}
        self.actionSubscribers = {}
        self.actionPublishers = {}
        self.actionRpcClients = {}
        self.publisher = None
        self.logger = logging.getLogger(__name__)

        self.commlib_node = CommlibNode(node_name=f"${time.time()}_commlib_node",
            connection_params=RedisConnectionParameters(),
            heartbeats=False,
            debug=True
        )
        self.commlib_node.run()

    def startSubscriber(self, action, broker, callback):
        # Check if topic with the specific broker is already subscribed
        if action['topic'] in self.actionSubscribers and \
            self.actionSubscribers[action['topic']]['broker']['parameters']['host'] == broker['parameters']['host']:

            self.logger.warning("Subscriber already exists for action: %s", action['topic'])
            return

        self.logger.info("Creating subscriber for action: %s", action['topic'])
        actionSubscriber = self.commlib_node.create_subscriber(
            topic=action['topic'],
            on_message=callback
        )

        self.actionSubscribers[action['topic']] = {
            "subscriber": actionSubscriber,
            "broker": broker
        }

        actionSubscriber.run()
        self.logger.info("Subscriber created and started")

    def stopSubscriber(self, action, broker):
        if action['topic'] in self.actionSubscribers and \
            self.actionSubscribers[action['topic']]['broker']['parameters']['host'] == broker['parameters']['host']:

            self.actionSubscribers[action['topic']]['subscriber'].stop()
            del self.actionSubscribers[action['topic']]
            self.logger.info("Subscriber stopped for action: %s", action['topic'])
        else:
            self.logger.error("Active subscriber not found for action: %s", action['topic'])

    def actionPublish(self, action, broker, parameters):
        if action['topic'] not in self.actionPublishers:
            # Add it
            self.logger.info("Creating publisher for action: %s", action['topic'])

            actionPublisher = self.commlib_node.create_publisher(
                topic=action['topic']
            )
            actionPublisher.run()

            self.actionPublishers[action['topic']] = {
                "publisher": actionPublisher,
                "broker": broker,
                "initial_payload": action['payload']
            }

        self.logger.info("Publishing the action")
        # Handle the the payload
        payload = copy.deepcopy(self.actionPublishers[action['topic']]['initial_payload'])
        self.logger.info("\tPayload: %s", payload)
        # iterate through the payload and replace the variables
        payload = self.iteratePayload(payload, parameters)
        self.logger.info("\tIterated Payload: %s", payload)
        # publish it
        self.actionPublishers[action['topic']]['publisher'].publish(payload)

    def actionRPCCall(self, action, broker, parameters):
        if action['topic'] not in self.actionRpcClients:
            action_rpc_call = self.commlib_node.create_rpc_client(
                rpc_name=action['topic']
            )
            action_rpc_call.run()

            self.actionRpcClients[action['topic']] = {
                "rpc": action_rpc_call,
                "broker": broker,
                "initial_payload": action['payload']
            }

        self.logger.info("RPC calling the action")
        # Handle the the payload
        payload = copy.deepcopy(self.actionRpcClients[action['topic']]['initial_payload'])
        self.logger.info("\tPayload: %s", payload)
        # iterate through the payload and replace the variables
        payload = self.iteratePayload(payload, parameters)
        self.logger.info("\tIterated Payload: %s", payload)
        # publish it
        self.actionRpcClients[action['topic']]['rpc'].call(payload)

    def iteratePayload(self, payload, parameters):
        for key, value in payload.items():
            if isinstance(value, dict): # or list
                payload = self.iteratePayload(value, parameters)
            else:
                # Search for variables in the parameters
                pattern = r'\{([^}]*)\}'
                self.logger.info("\tPattern: %s", pattern)
                self.logger.info("\tValue: %s", value)
                matches = re.findall(pattern, str(value))
                self.logger.info("\tMatches: %s", matches)
                for match in matches:
                    for p in parameters:
                        if p['id'] == match:
                            self.logger.info("\tMatch found: %s", match)
                            value = value.replace("{" + match + "}", str(p['value']))
                            self.logger.info("\tValue replaced: %s", value)
                            payload[key] = self.evaluate(value)
                            self.logger.info("\tPayload: %s", payload)
        return payload

    def setPublisher(self, publisher):
        """
        Set the publisher of the app.

        Args:
            publisher (str): The name of the publisher.

        Returns:
            None
        """
        self.publisher = publisher

    def get(self, key):
        """
        Retrieve the value associated with the given key.

        Args:
            key (str): The key to retrieve the value for.

        Returns:
            The value associated with the key, or None if the key does not exist.
        """
        return self.storage.get(key)

    def set(self, key, value):
        """
        Set the value for the given key.

        Args:
            key (str): The key to set the value for.
            value: The value to be stored.

        Returns:
            True if the value was successfully set.
        """
        self.storage[key] = value
        if self.publisher is not None:
            self.publisher.publish({
                "type": "storage",
                "action": "set",
                "key": key,
                "value": value
            })
        return True

    def delete(self, key):
        """
        Delete the value associated with the given key.

        Args:
            key (str): The key to delete the value for.

        Returns:
            True if the value was successfully deleted, False if the key does not exist.
        """
        if key in self.storage:
            del self.storage[key]
            return True
        return False
    
    def evaluate(self, expression):
        """
        Evaluate an expression containing variables stored in the storage.

        Args:
            expression (str): The expression to evaluate.

        Returns:
            The result of the expression, or None if an error occurred during evaluation.
        """
        try:
            # Make the expression a string
            expression = str(expression)
            self.logger.info("- Evaluating expression: %s", expression)
            pattern = r'\{([^}]*)\}'
            matches = re.findall(pattern, expression)
            for match in matches:
                variable_value = self.get(match)
                if variable_value is not None:
                    expression = expression.replace("{" + match + "}", str(variable_value))
            self.logger.info("- Evaluated expression: %s", expression)
            return eval(expression)
        except Exception as e: # pylint: disable=broad-except
            self.logger.error("- Error during evaluation: %s", e)
            return None

    def replaceVariables(self, expression):
        """
        Replace variables in an expression with their values.

        Args:
            expression (str): The expression to replace variables in.

        Returns:
            The expression with variables replaced by their values.
        """
        pattern = r'\{([^}]*)\}'
        matches = re.findall(pattern, expression)
        for match in matches:
            variable_value = self.get(match)
            if variable_value is not None:
                expression = expression.replace("{" + match + "}", str(variable_value))
        return expression

    def stop(self):
        """
        Stop the storage handler.

        Args:
            None

        Returns:
            None
        """
        try:
            self.logger.info("Stopping subscribers and publishers")
            for action in self.actionSubscribers:
                self.actionSubscribers[action]['subscriber'].stop()
            for action in self.actionPublishers:
                self.actionPublishers[action]['publisher'].stop()
        except: # pylint: disable=bare-except
            self.logger.error("Error stopping subscribers")

        self.actionSubscribers = {}
        self.actionPublishers = {}
        self.publisher = None
        self.storage = {}   