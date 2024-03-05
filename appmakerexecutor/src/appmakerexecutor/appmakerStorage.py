"""
This module provides a StorageHandler class for managing key-value storage.
"""

import re
import time

from commlib.node import Node as CommlibNode
from commlib.transports.mqtt import ConnectionParameters

class StorageHandler:
    """
    A class for managing key-value storage.
    """
    def __init__(self):
        self.storage = {}
        self.actionSubscribers = {}
        self.actionPublishers = {}
        self.publisher = None

    def startSubscriber(self, action, broker, callback):
        # Check if topic with the specific broker is already subscribed
        if action['topic'] in self.actionSubscribers and \
            self.actionSubscribers[action['topic']]['broker']['parameters']['host'] == broker['parameters']['host']:
                print("Subscriber already exists for action: ", action['topic'])
                return

        conn_params = ConnectionParameters(
            host=broker['parameters']['host'],
            port=broker['parameters']['port'],
            username=broker['parameters']['username'],
            password=broker['parameters']['password']
        )
        print("Creating subscriber for action: ", action['topic'])
        print("Connection parameters: ", conn_params)

        commlib_node = CommlibNode(node_name=f"${time.time()}_commlib_node",
            connection_params=conn_params,
            heartbeats=False,
            debug=True
        )

        actionSubscriber = commlib_node.create_subscriber(
            topic=action['topic'],
            on_message=callback
        )

        self.actionSubscribers[action['topic']] = {
            "subscriber": actionSubscriber,
            "broker": broker
        }

        print("Executing the subscriber")
        actionSubscriber.run()

        print("Subscriber created and started")

    def stopSubscriber(self, action, broker):
        if action['topic'] in self.actionSubscribers and \
            self.actionSubscribers[action['topic']]['broker']['parameters']['host'] == broker['parameters']['host']:
                self.actionSubscribers[action['topic']]['subscriber'].stop()
                del self.actionSubscribers[action['topic']]
                print("Subscriber stopped for action: ", action['topic'])
        else:
            print("Active subscriber not found for action: ", action['topic'])

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
        # print("Value set: ", key, " ", value)
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
            print("Evaluating expression: ", expression)
            pattern = r'\{([^}]*)\}'
            matches = re.findall(pattern, expression)
            for match in matches:
                variable_value = self.get(match)
                if variable_value is not None:
                    expression = expression.replace("{" + match + "}", str(variable_value))
            print("Evaluated expression: ", expression)
            return eval(expression)
        except Exception as e:
            print("Error during evaluation: ", e)
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
        for action in self.actionSubscribers:
            self.actionSubscribers[action]['subscriber'].stop()
        
        self.actionSubscribers = {}
        self.actionPublishers = {}
        self.publisher = None
        self.storage = {}   