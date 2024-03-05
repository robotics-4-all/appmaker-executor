import time
import random
import re
import pprint

from commlib.node import Node as CommlibNode
from commlib.transports.mqtt import ConnectionParameters

class Node:
    def __init__(self, data, publisher=None, storageHandler=None, brokers=[]):
        self.data = data
        self.publisher = publisher
        self.storageHandler = storageHandler
        self.id = data['id']
        self.label = data['data']['label']
        self.toolbox = data['data']['toolbox']
        self.count = data['data']['count']
        self.parameters = data['data']['parameters'] if 'parameters' in data['data'] else []
        self.connections = {}
        self.connection_list = []
        self.brokers = brokers
        self.actionPublisher = None
        self.actionSubscriber = None
        self.actionVariable = None
        self.commlib_node = None
        self.is_preempted = False
        # In case of thread split, we need to keep the executors
        self.executors = {}
        # In case of thread join, we need to keep the next join node
        self.nextJoin = None
        # In case of preempt, we need to keep the executor to kill
        self.executor_to_preempt = None

        pprint.pprint(data)

    def addConnection(self, node, connection):
            """
            Adds a connection to the node.

            Args:
                node: The node to connect to.
                connection: The connection details.

            Returns:
                None
            """
            self.connections[node.id] = connection
            self.connection_list.append(connection['target'])

    def publish(self, message):
            """
            Publishes a message using the publisher associated with this node.

            Args:
                message (str): The message to be published.

            Returns:
                None
            """
            if self.publisher != None:
                self.publisher.publish({
                    "node_id": self.id,
                    "message": message,
                    "label": self.label,
                })

    def on_message(self, message):
        if self.actionVariable:
            self.storageHandler.set(self.actionVariable, message)

    def execute(self):
            """
            Executes the logic of the current node and returns the next node to be executed.

            Returns:
                next_node: The next node to be executed.
            """
            next_node = None
            if self.is_preempted:
                print("Node: ", self.id, " ", self.label, " is preempted")
                return None

            self.publish("start")
            if self.label == "Condition":
                next_node = self.executeCondition()
            elif self.label == "Random":
                next_node = self.executeRandom()
            elif self.label == "End":
                next_node = None
            elif self.label == "Thread split":
                next_node = self.executeThreadSplit()
            elif self.label == "Preempt":
                next_node = self.executePreempt()
            elif self.label == "Delay":
                next_node = self.executeDelay()
            elif self.label == "Create variable" or self.label == "Set variable":
                next_node = self.executeSetVariable()
            elif self.label == "Log":
                next_node = self.executeLog()
            else: # All other nodes
                next_node = self.executeGeneral()

            # Handle actions
            if 'action' in self.data['data']:
                broker_id = None
                operation = None
                action = self.data['data']['action']

                # Subscriber
                if action['type'] == 'subscribe':
                    # Find the broker id:
                    for p in self.data['data']['parameters']:
                        if p['id'] == 'broker':
                            broker_id = p['value']
                            break
                    # Get broker
                    correct_broker = None
                    for b in self.brokers:
                        if b["id"] == broker_id:
                            correct_broker = b
                            break
                    # Find the operation. Start or stop?
                    for p in self.data['data']['parameters']:
                        if p['id'] == 'operation':
                            operation = p['value']
                            break

                    self.actionVariable = action['storage']

                    print("The correct broker is: ", correct_broker['parameters']['name'])
                    if broker_id and correct_broker and operation == "start":
                        self.storageHandler.startSubscriber(
                            action, 
                            correct_broker,
                            self.on_message,
                        )
                    elif broker_id and correct_broker and operation == "stop":
                        self.storageHandler.stopSubscriber(
                            action,
                            correct_broker,
                        )
                    else:
                        print("Something went wrong with action: ", action, correct_broker, operation)

                elif action['type'] == "publish":
                    for p in self.data['data']['parameters']:
                        if p['id'] == 'broker':
                            broker_id = p['value']
                            break
                    # Get broker
                    correct_broker = None
                    for b in self.brokers:
                        if b["id"] == broker_id:
                            correct_broker = b
                            break

                    self.storageHandler.actionPublish(
                        action,
                        correct_broker,
                        self.data['data']['parameters'],
                    )

            self.publish("end")
            return next_node
    
    def executeLog(self):
        """
        Executes the log operation.

        This method logs the message to the console and returns the key of the first connection.

        Returns:
            str: The key of the first connection.
        """
        time.sleep(1)
        message = self.parameters[0]['value']
        message = self.storageHandler.replaceVariables(message)
        print("Log: ", message)
        # Get current time in literal format
        timestamp = time.strftime("%H:%M:%S", time.localtime())
        self.publish({
            "message": message, 
            "timestamp": timestamp,
            "node_count": self.count,
        })
        return list(self.connections.keys())[0]

    def executeSetVariable(self):
        """
        Executes the set variable operation.
        
        Sets the value of a variable with the given name to the evaluated value of the provided expression.
        
        Returns:
            str: The key of the first connection.
        """
        time.sleep(1)
        variable_name = self.parameters[0]['value']
        variable_value = self.parameters[1]['value']
        evaluated = self.storageHandler.evaluate(variable_value)
        print("Setting variable: ", variable_name, " ", evaluated)
        self.storageHandler.set(variable_name, evaluated)
        return list(self.connections.keys())[0]
    
    def executeCondition(self):
        """
        Executes the condition of the node and returns the next node to be executed.

        This method evaluates the conditions specified in the node's parameters and selects the next node
        based on the first condition that evaluates to True. If none of the conditions evaluate to True,
        the method returns None.

        Returns:
            str: The ID of the next node to be executed.
        """
        # Select one of the outputs at random
        print("Executing node: ", self.id, " ", self.label)
        next_node_index = 0
        for p in self.parameters:
            print(">>", p['id'], " ", p['value'])
            # Evaluate the condition
            result = False
            try:
                result = self.storageHandler.evaluate(str(p['value']))
                print("Result: ", result)
            except Exception as e:
                print("Error in evaluating the condition: ", e) 
            if result:
                break
            next_node_index += 1 
        time.sleep(1)
        print("Selected form condition: ", next_node_index)
        return list(self.connections.keys())[next_node_index]

    def executeRandom(self):
        """
        Executes the node by randomly selecting one of the outputs based on the probabilities assigned to each output.

        Returns:
            The selected output connection.
        """
        # Select one of the outputs at random
        print("Executing node: ", self.id, " ", self.label)
        time.sleep(1)
        # Gather all the parameters and evaluate them
        probabilities = [self.storageHandler.evaluate(x['value']) for x in self.parameters]
        prob_sum = sum(probabilities)
        random_prob = random.uniform(0, prob_sum)
        print(self.connection_list)
        print("Random probability: ", random_prob)
        for i in range(len(probabilities)):
            if random_prob < probabilities[i]:
                print("Selected: ", i)
                return self.connection_list[i]
            random_prob -= probabilities[i]
        print("Something went wrong, returning the last connection")
        return self.connection_list[-1]
    
    def executeThreadSplit(self):
        """
        Executes the node in a threaded manner.

        This method starts the executors threaded and waits for them to finish.
        It prints the node ID and label before executing the threads.
        """
        # We must start the executors threaded
        time.sleep(1)
        print("Executing node: ", self.id, " ", self.label)
        if self.executors:
            print("Executing threads")
            for e in self.executors:
                self.executors[e].finished = False
                self.executors[e].executeThreaded()
            print("Waiting for threads to finish") 
            while True:
                time.sleep(0.1)
                if all([self.executors[e].finished for e in self.executors]):
                    print("Threads finished")
                    break
        return self.nextJoin
    
    def executePreempt(self):
        """
        Executes the node in a preemptive manner.

        This method enforces preemption by sleeping for 1 second and then
        calling the `enforcePreemption` method of the `executor_to_preempt`
        object. It returns the key of the first connection in the `connections`
        dictionary.

        Returns:
            str: The key of the first connection in the `connections` dictionary.
        """
        print("Executing node: ", self.id, " ", self.label)
        time.sleep(1)
        self.executor_to_preempt.enforcePreemption()
        return list(self.connections.keys())[0]
    
    def executeDelay(self):
        """
        Executes the delay node by waiting for the specified delay time.

        Returns:
            str: The ID of the next connected node.
        """
        # Wait for the delay time
        print("Executing node: ", self.id, " ", self.label)
        print(self.parameters)
        if 'value' not in self.parameters[0]:
            print("Delay parameter not found")
            return None
        delay = self.storageHandler.evaluate(self.parameters[0]['value'])
        print("Delay: ", delay)
        time.sleep(float(delay))
        return list(self.connections.keys())[0]
    
    def executeGeneral(self):
        """
        Executes the general node.

        This method prints the node ID and label, sleeps for 1 second, and returns the key of the first connection.

        Returns:
            str: The key of the first connection.
        """
        print("Executing node: ", self.id, " ", self.label)
        time.sleep(1)
        return list(self.connections.keys())[0]

    def printNode(self):
        """
        Print information about the node, including its ID, label, count, parameters, and connections.
        """
        print("Node: ", self.id, " ", self.label, " ", self.count)
        print("Parameters: ")
        for p in self.parameters:
            print("\t", p['id'], " ", p['value'] if 'value' in p else "")
        print("Connections: ")
        for c in self.connections:
            print("\tto ", c)

