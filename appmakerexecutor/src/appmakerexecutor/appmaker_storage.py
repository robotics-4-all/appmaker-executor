"""
This module provides a StorageHandler class for managing key-value storage.
"""

import re
import time
import logging
import copy
import json
import pprint

from commlib.node import Node as CommlibNode
from commlib.transports.redis import ConnectionParameters as RedisConnectionParameters

class StorageHandler:
    """
    A class to handle storage operations, communication with subscribers,
    publishers, and RPC clients.
    Attributes:
        storage (dict): A dictionary to store key-value pairs.
        subscribers (dict): A dictionary to store subscribers.
        publishers (dict): A dictionary to store publishers.
        rpc_clients (dict): A dictionary to store RPC clients.
        logger (logging.Logger): A logger instance for logging messages.
        commlib_node (CommlibNode): A communication library node for handling communication.
    Methods:
        __init__():
            Initializes the StorageHandler instance.
        startSubscriber(action, broker, callback):
            Starts a subscriber for a given action and broker.
        stopSubscriber(action, broker):
            Stops a subscriber for a given action and broker.
        actionPublish(action, broker, parameters):
            Publishes an action to a given broker with specified parameters.
        actionRPCCall(action, broker, parameters):
            Makes an RPC call for a given action and broker with specified parameters.
        iteratePayload(payload, parameters):
            Iterates through the payload and replaces variables with their values.
        setPublisher(publisher):
            Sets the publisher of the app.
        get(key):
            Retrieves the value associated with the given key.
        set(key, value):
            Sets the value for the given key.
        delete(key):
            Deletes the value associated with the given key.
        evaluate(expression):
            Evaluates an expression containing variables stored in the storage.
        replaceVariables(expression):
            Replaces variables in an expression with their values.
        stop():
            Stops the storage handler.
    """
    def __init__(self, uid, model, stop_publisher):
        self.storage = {}
        self.subscribers = {}
        self.publishers = {}
        self.rpc_clients = {}
        self.action_clients = {}
        self.publisher = None
        self.model = model
        self.uid = uid
        self.goaldsl_id = None
        self.stop_publisher = stop_publisher
        self.logger = logging.getLogger(__name__)

        self.commlib_node = CommlibNode(node_name=f"${time.time()}_commlib_node",
            connection_params=RedisConnectionParameters(
                socket_timeout=60,
            ),
            heartbeats=False,
            debug=True,
        )

        self.goaldsl_subscriber = self.commlib_node.create_psubscriber(
            topic="goaldsl.*.event",
            on_message=self.handle_goaldsl_message
        )

        self.streamsim_start_rpc_client = self.commlib_node.create_rpc_client(
            rpc_name=f"streamsim.{self.uid}.set_configuration_local",
        )

        self.streamsim_reset_rpc_client = self.commlib_node.create_rpc_client(
            rpc_name=f"streamsim.{self.uid}.reset",
        )

        self.goaldsl_start_rpc = self.commlib_node.create_rpc_client(
            rpc_name=f"goaldsl.{self.uid}.deploy_sync",
        )

        self.goaldsl_reset_rpc = self.commlib_node.create_rpc_client(
            rpc_name=f"goaldsl.{self.uid}.killall_sync",
        )

        self.variables_publisher = self.commlib_node.create_publisher(
            topic="appcreator.variables"
        )

        self.scores_publisher = self.commlib_node.create_publisher(
            topic="appcreator.scores.internal"
        )

        self.identify_subscribers_and_start_them()

        self.commlib_node.run()

    def fix_topic(self, topic):
        """
        Modifies the given topic by replacing the second segment with the instance's UID.
        Args:
            topic (str): The topic string to be modified. It is expected to be in the 
            format 'segment1.segment2.segment3...'.

        Returns:
            str: The modified topic string with the second segment replaced by the 
            instance's UID.
        """
        if "streamsim." in topic:
            tt = topic.split(".")
            tt[1] = self.uid
            return ".".join(tt)
        return topic

    def identify_subscribers_and_start_them(self):
        """
        Identify subscribers and start them.
        """
        self.logger.info("Identifying subscribers and starting them")
        nodes_str = json.dumps(self.model['nodes'])
        for toolbox in self.model['toolboxes']:
            for node in toolbox['nodes']:
                if 'action' in node and node['action']['type'] == "subscribe":
                    node['action']['topic'] = self.fix_topic(node['action']['topic'])
                    topic = node['action']['topic']
                    variable = node['action']['storage']
                    if 'literalVariables' in node:
                        for literal in node['literalVariables']:
                            if literal in nodes_str:
                                # The variable is used, start the subscriber
                                # Dynamically create a callback in the class
                                topic = topic.replace(".", "_")
                                callback = lambda message, var=variable: self.set(var, message)
                                self.start_subscriber(
                                    node['action'],
                                    None,
                                    callback,
                                    literal
                                )

                                break # In case other literals are used, sub has started
        # exit(0)

    def start_simulation(self, model):
        """
        Start the simulation.

        Args:
            model (dict): The model to use for the simulation.

        Returns:
            None
        """
        self.logger.info("Starting simulation")
        yamlmodel = json.loads(model)
        self.streamsim_start_rpc_client.call(yamlmodel)

    def reset_simulation(self):
        """
        Reset the simulation.

        Args:
            None

        Returns:
            None
        """
        self.logger.info("Resetting simulation")
        self.streamsim_reset_rpc_client.call({})
        time.sleep(2)

    def deploy_goaldsl(self, model):
        """
        Start the goaldsl model.

        Args:
            model (dict): The model to use for the simulation.

        Returns:
            None
        """
        self.logger.info("Starting goaldsl")
        tmp = model[1:-1].replace("\\n", "\n").replace("\\t", "\t").\
            replace("\\\"", "\"").replace("\\'", "'").replace("\\\\", "\\")
        self.goaldsl_start_rpc.call({'model': tmp})
        time.sleep(2)

    def stop_goaldsl(self):
        """
        Reset the simulation.

        Args:
            None

        Returns:
            None
        """
        time.sleep(1)
        self.logger.info("Resetting goaldsl")
        self.goaldsl_reset_rpc.call({})

    def handle_goaldsl_message(self, message, _):
        """
        Handle messages received on the goaldsl topic.

        Args:
            message (dict): The message received on the goaldsl topic.

        Returns:
            None
        """
        self.logger.info("Received message on goaldsl topic: %s", message)
        if "type" in message and message["type"] in \
                ["scenario_update", "scenario_started", "scenario_finished"]:
            if self.publisher is not None:
                self.publisher.publish({
                    "node_id": None,
                    "message": message,
                    "label": "Score",
                    "timestamp": time.time(),
                })

                self.scores_publisher.publish({
                    "user_id": self.uid,
                    "goaldsl_id": self.goaldsl_id,
                    "update": message
                })

            if message["type"] == "scenario_finished":
                # check for fatals
                if "fatal_goals" in message["data"]:
                    # Iterate through the fatal goals
                    for fatal_goal in message["data"]["fatal_goals"]:
                        if fatal_goal["state"] == "COMPLETED":
                            print("Fatal goal completed")
                            if self.stop_publisher is not None:
                                # Inform UI for stop
                                self.publisher.publish({
                                    "node_id": None,
                                    "message": "runtime_error: Fatal goal triggered!",
                                    "label": None,
                                    "timestamp": time.time(),
                                })
                                print("Sent end to UI")
                                time.sleep(1)
                                self.stop_publisher.publish({
                                    "node_id": None,
                                    "message": "Fatal goal triggered!",
                                    "label": "Fatal",
                                })
                                time.sleep(1)

    def start_subscriber(self, action, broker, callback, literal = None):
        """
        Starts a subscriber for a given action and broker.

        This method checks if a subscriber for the specified topic and broker
        already exists. If it does, a warning is logged and the method returns.
        Otherwise, it creates a new subscriber, stores it in the subscribers
        dictionary, and starts the subscriber.

        Args:
            action (dict): A dictionary containing the action details, including
                           the 'topic' key which specifies the topic to subscribe to.
            broker (dict): A dictionary containing the broker details, including
                           the 'parameters' key which contains the broker's parameters.
            callback (function): A callback function to be called when a message is
                                 received on the subscribed topic.

        Returns:
            None
        """
        # Check if topic with the specific broker is already subscribed
        _topic = self.fix_topic(action['topic'])
        if _topic in self.subscribers and \
            self.subscribers[_topic]['broker']['parameters']['host'] \
                == broker['parameters']['host']:

            self.logger.warning("Subscriber already exists for action: %s", _topic)
            return

        self.logger.critical("Creating subscriber for action: %s : %s", _topic, literal)
        _subscriber = self.commlib_node.create_subscriber(
            topic=_topic,
            on_message=callback
        )

        self.subscribers[_topic] = {
            "subscriber": _subscriber,
            "broker": broker,
            "literal": literal
        }

        _subscriber.run()
        self.logger.info("Subscriber created and started")

    def stop_subscriber(self, action, broker):
        """
        Stops the subscriber for a given action if it exists and matches the specified broker.

        Args:
            action (dict): A dictionary containing the action details, including the 'topic'.
            broker (dict): A dictionary containing the broker details, including 'parameters' and 
                'host'.

        Logs:
            Info: Logs a message indicating that the subscriber was stopped for the given action.
            Error: Logs an error message if no active subscriber is found for the given action.
        """
        _topic = self.fix_topic(action['topic'])
        if _topic in self.subscribers and \
            self.subscribers[_topic]['broker']['parameters']['host'] \
                == broker['parameters']['host']:

            self.subscribers[_topic]['subscriber'].stop()
            del self.subscribers[_topic]
            self.logger.info("Subscriber stopped for action: %s", _topic)
        else:
            self.logger.error("Active subscriber not found for action: %s", _topic)

    def action_publish(self, action, broker, parameters):
        """
        Publishes an action to a specified topic using a publisher.

        If a publisher for the given action topic does not exist, it creates one and stores it.
        Then, it deep copies the initial payload, iterates through it to replace 
        variables with the provided parameters, and publishes the modified payload.

        Args:
            action (dict): A dictionary containing the action details, including the 
            'topic' and 'payload'.
            broker (str): The broker information associated with the publisher.
            parameters (dict): A dictionary of parameters to replace variables in the payload.

        Returns:
            None
        """
        _topic = self.fix_topic(action['topic'])
        if _topic not in self.publishers:
            # Add it
            self.logger.info("Creating publisher for action: %s", _topic)
            _topic = self.fix_topic(_topic)
            _publisher = self.commlib_node.create_publisher(
                topic=_topic
            )
            _publisher.run()

            self.publishers[_topic] = {
                "publisher": _publisher,
                "broker": broker,
                "initial_payload": action['payload']
            }

        self.logger.info("Publishing the action")
        # Handle the the payload
        payload = copy.deepcopy(self.publishers[_topic]['initial_payload'])
        self.logger.info("\tPayload: %s", payload)
        # iterate through the payload and replace the variables
        payload = self.iterate_payload(payload, parameters)
        self.logger.info("\tIterated Payload: %s", payload)
        # publish it
        self.publishers[_topic]['publisher'].publish(payload)

    def action_rpc_call(self, action, broker, parameters):
        """
        Handles the RPC call for a given action.

        This method creates an RPC client if one does not already exist for the given action's 
        topic.
        It then prepares the payload, iterates through it to replace variables with the provided 
        parameters,
        and makes the RPC call with the prepared payload.

        Args:
            action (dict): A dictionary containing the action details, including 'topic' and 
                'payload'.
            broker (str): The broker information associated with the action.
            parameters (dict): A dictionary of parameters to replace variables in the payload.

        Returns:
            response: The response from the RPC call.

        Logs:
            - Creation of the RPC client for the action's topic.
            - The initial payload before iteration.
            - The iterated payload after replacing variables.
            - The response from the RPC call.
        """
        _topic = self.fix_topic(action['topic'])
        if _topic not in self.rpc_clients:
            _rpc_call = self.commlib_node.create_rpc_client(
                rpc_name=_topic,
            )
            _rpc_call.run()
            self.logger.info("Creating RPC client for action: %s", _topic)

            self.rpc_clients[_topic] = {
                "rpc": _rpc_call,
                "broker": broker,
                "initial_payload": action['payload']
            }

        self.logger.info("RPC calling the action")
        # Handle the the payload
        payload = copy.deepcopy(self.rpc_clients[_topic]['initial_payload'])
        self.logger.info("\tPayload: %s", payload)
        # iterate through the payload and replace the variables
        payload = self.iterate_payload(payload, parameters)
        self.logger.info("\tIterated Payload: %s", payload)
        # publish it
        response = self.rpc_clients[_topic]['rpc'].call(
            payload,
            timeout=120,
        )
        self.logger.info("RPC called with response: %s", response)
        return response

    def action_action_call(self, action, broker, parameters):
        """
        Handles the execution of an action call.

        This method checks if an action client for the given action topic exists.
        If not, it creates a new action client, runs it, and stores it in the action_clients 
        dictionary.
        It then prepares the payload by deep copying the initial payload and iterating through it 
        to replace variables.
        Finally, it sends the goal to the action client and waits for the result.

        Args:
            action (dict): A dictionary containing the action details, including the 'topic' and 
            'payload'.
            broker (object): The broker object associated with the action.
            parameters (dict): A dictionary of parameters to be used for replacing variables in 
            the payload.

        Returns:
            object: The result of the action call.
        """
        _topic = self.fix_topic(action['topic'])
        if _topic not in self.action_clients:
            _action_call = self.commlib_node.create_action_client(
                action_name=_topic,
            )
            _action_call.run()
            self.logger.info("Creating Action client for action: %s", _topic)

            self.action_clients[_topic] = {
                "action": _action_call,
                "broker": broker,
                "initial_payload": action['payload']
            }

        self.logger.info("Action calling the action")
        # Handle the the payload
        payload = copy.deepcopy(self.action_clients[_topic]['initial_payload'])
        self.logger.info("\tPayload: %s", payload)
        # iterate through the payload and replace the variables
        payload = self.iterate_payload(payload, parameters)
        self.logger.info("\tIterated Payload: %s", payload)
        # publish it
        self.action_clients[_topic]['action'].send_goal(
            payload
        )
        self.logger.info("Action called")
        response = self.action_clients[_topic]['action'].get_result(wait=True, \
            timeout=120, wait_max_sec=120)
        return response

    def iterate_payload(self, payload, parameters):
        """
        Recursively iterates through a payload dictionary, replacing placeholders with corresponding
        values from parameters.

        Args:
            payload (dict): The dictionary containing the payload data with placeholders.
            parameters (list): A list of dictionaries, each containing 'id' and 'value' keys, used 
                to replace placeholders in the payload.

        Returns:
            dict: The updated payload dictionary with placeholders replaced by corresponding values
                from parameters.

        Example:
            payload = {
            "key1": "value1",
            "key2": "{param1}",
            "key3": {
                "subkey1": "{param2}"
            }
            }
            parameters = [
            {"id": "param1", "value": "replaced_value1"},
            {"id": "param2", "value": "replaced_value2"}
            ]
            result = iteratePayload(payload, parameters)
            # result will be:
            # {
            #     "key1": "value1",
            #     "key2": "replaced_value1",
            #     "key3": {
            #         "subkey1": "replaced_value2"
            #     }
            # }
        """
        for key, value in payload.items():
            if isinstance(value, dict): # or list
                payload = self.iterate_payload(value, parameters)
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
                            if payload[key] is None: # Evaluation failed, not a numeric value
                                payload[key] = str(value)
                            self.logger.info("\tPayload: %s", payload)
        return payload

    def set_publisher(self, publisher):
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

        variables_publish_payload = {
            "name": key,
            "value": value,
            "type": type(value).__name__
        }
        self.variables_publisher.publish(variables_publish_payload)
        print(f"Published: {variables_publish_payload}")
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

    def handle_variable_string(self, expression):
        """
        Check if the items in the expression can be evaluated.

        Args:
            expression (str): The expression to check.

        Returns:
            True if the items in the expression can be evaluated, False otherwise.
        """
        value = {} # To suppress the warning
        try:
            self.logger.info("Handling this variable: %s", expression)
            items = expression.split(".")
            for i, item in enumerate(items):
                # Regex for array[integer] item
                match = re.match(r"(.+)\[(\d+)\]", item)
                if i == 0:
                    # if match:
                    #     array_name, index = match.groups()
                    #     index = int(index)
                    #     value = self.get(array_name)[index]
                    # else:
                    value = self.get(item)
                else:
                    try:
                        item = int(item)
                    except: # pylint: disable=bare-except
                        pass
                    value = value[item]
                    # check if it is number, else put it on quotes
                    if not isinstance(value, (int, float, list, dict)):
                        value = f'"{value}"'
        except Exception as e: # pylint: disable=broad-except
            self.logger.error("Error during evaluation: %s", e)

        return value

    def evaluate(self, expression):
        """
        Evaluate an expression containing variables stored in the storage.

        Args:
            expression (str): The expression to evaluate.

        Returns:
            The result of the expression, or None if an error occurred during evaluation.
        """
        try:
            expression = self.replace_variables(expression)
            return eval(expression) # pylint: disable=eval-used
        except Exception as e: # pylint: disable=broad-except
            self.logger.info("- Value %s could not be evaluated. Probably a string: %s", \
                expression, e)
            return expression

    def replace_variables(self, expression):
        """
        Replace variables in an expression with their values.

        Args:
            expression (str): The expression to replace variables in.

        Returns:
            The expression with variables replaced by their values.
        """
        try:
            # Make the expression a string
            expression = str(expression)
            self.logger.info("- Evaluating expression: %s", expression)
            pattern = r'\{([^}]*)\}'
            matches = re.findall(pattern, expression)
            for match in matches:
                variable_value = self.handle_variable_string(match)
                if variable_value is not None:
                    self.logger.info("- Variable value is %s", variable_value)
                    if isinstance(variable_value, str):
                        variable_value = f'"{variable_value}"'
                    expression = expression.replace("{" + match + "}", str(variable_value))
            self.logger.info("- Replaced expression: %s", expression)

            # Handle evaluations
            pattern = r"\|(.*?)\|"
            matches = re.findall(pattern, expression)
            for match in matches:
                self.logger.info("Handling this evaluation: %s", match)
                variable_value = eval(match) # pylint: disable=eval-used
                if variable_value is not None:
                    expression = expression.replace("|" + match + "|", str(variable_value))
            self.logger.info("- Evaluated expression: %s", expression)

            return expression
        except Exception as e: # pylint: disable=broad-except
            self.logger.error("- Error during evaluation: %s", e)
            return None

    def stop(self):
        """
        Stop the storage handler.

        Args:
            None

        Returns:
            None
        """
        try:
            self.logger.info("Stopping commlib node")
            self.commlib_node.stop()
        except: # pylint: disable=bare-except
            self.logger.error("Error stopping subscribers")

        self.subscribers = {}
        self.publishers = {}
        self.rpc_clients = {}
        self.publisher = None
        self.storage = {}
