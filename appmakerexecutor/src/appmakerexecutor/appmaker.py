from pprint import pprint

from commlib.node import Node as CommlibNode
from commlib.transports.mqtt import ConnectionParameters

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
    except Exception as e:
        print("Error on message: ", e)

if __name__ == "__main__":
    """
    This script initializes and runs the AppMakerExecutor.
    """
    conn_params = ConnectionParameters(
        host="locsys.issel.ee.auth.gr",
        port=8883,
        ssl=True,
        username="sensors",
        password="issel.sensors"
    )

    commlib_node = CommlibNode(node_name='locsys.app_executor_node',
        connection_params=conn_params,
        heartbeats=False,
        debug=True)

    commlib_node.create_subscriber(
        topic="locsys.app_executor.deploy",
        on_message=on_message
    )

    try:
        commlib_node.run_forever()
    except Exception as e:
        print("Error: ", e)
        commlib_node.stop()
