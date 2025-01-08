"""
File that initializes an AppMakerExecutor.
"""

import os
import sys
from pprint import pprint
from dotenv import load_dotenv
import logging

from commlib.node import Node as CommlibNode
from commlib.transports.mqtt import ConnectionParameters as MQTTConnectionParameters

from appmakerexecutor import AppMakerExecutor

def on_message(message):
    """
    Handles incoming messages.

    Args:
        message (dict): The message received.

    Returns:
        None
    """
    try:
        print("Received model")
        print("Feedback on:", message['feedbackTopic'])
        amexe = AppMakerExecutor(feedback_topic=message['feedbackTopic'])
        pprint(message)
        amexe.load_model(message)
        amexe.execute()
        print("All done")
    except Exception as e: # pylint: disable=broad-except
        print("Error on message: ", e)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("You must provide a UID as argument:")
        print(">> python3 appmaker.py UID")
        exit(0)

    uid = sys.argv[1]

    load_dotenv()
    try:
        broker_host = os.getenv('BROKER_HOST', 'broker.emqx.io')
        broker_port = int(os.getenv('BROKER_PORT', "8883"))
        broker_ssl = bool(os.getenv('BROKER_SSL', "True"))
        broker_username = os.getenv('BROKER_USERNAME', '')
        broker_password = os.getenv('BROKER_PASSWORD', '')
    except Exception as e: # pylint: disable=broad-except
        print("Error: ", e)
        exit(1)

    conn_params = MQTTConnectionParameters(
        host=broker_host,
        port=broker_port,
        ssl=broker_ssl,
        username=broker_username,
        password=broker_password,
    )

    commlib_node = CommlibNode(node_name='locsys.app_executor_node',
        connection_params=conn_params,
        heartbeats=False,
        debug=True)

    commlib_node.create_subscriber(
        topic=f"appcreator.{uid}.deploy",
        on_message=on_message
    )
    print(f"Subscribed to appcreator.{uid}.deploy")

    try:
        commlib_node.run_forever()
    except Exception as e: # pylint: disable=broad-except
        print("Error: ", e)
        commlib_node.stop()
