import os
import platform
import time
import json
from main import Product, db

from solace.messaging.config.transport_security_strategy import TLS
from solace.messaging.messaging_service import MessagingService, ReconnectionListener, ReconnectionAttemptListener, ServiceInterruptionListener, ServiceEvent
from solace.messaging.resources.queue import Queue
from solace.messaging.config.retry_strategy import RetryStrategy
from solace.messaging.receiver.persistent_message_receiver import PersistentMessageReceiver
from solace.messaging.receiver.message_receiver import MessageHandler, InboundMessage
from solace.messaging.errors.pubsubplus_client_error import PubSubPlusClientError

if platform.uname().system == 'Windows': os.environ["PYTHONUNBUFFERED"] = "1" # Disable stdout buffer


# Handle received messages
class MessageHandlerImpl(MessageHandler):
    def on_message(self, message: InboundMessage):
        # Check if the payload is a String or Byte, decode if its the later
        payload = message.get_payload_as_string() if message.get_payload_as_string() != None else message.get_payload_as_bytes()
        if isinstance(payload, bytearray):
            print(f"Received a message of type: {type(payload)}. Decoding to string")
            payload = payload.decode()

        #payload = message.get_payload_as_dictionary()

        topic = message.get_destination_name()
        print("\n" + f"Message property dump: {message.get_properties()} \n")

        content_type = message.get_property('content-type')

        print("\n" + f"Message dump: {message} \n")
        print("\n" + f"Received message on: {topic}")
        print("\n" + f"Message payload: {payload} \n")
        #content_type = payload["content-type"]
        #print("content_type %s" % content_type)

        data = json.loads(payload)
        print("\n" + f"Message payload in json: {data} \n")

        if content_type == 'product_created':
            product = Product(id=data['id'], title=data['title'], image=data['image'])
            db.session.add(product)
            db.session.commit()
            print('Product Created')

        elif content_type == 'product_updated':
            product = Product.query.get(data['id'])
            product.title = data['title']
            product.image = data['image']
            db.session.commit()
            print('Product Updated')

        elif content_type == 'product_deleted':
            product = Product.query.get(data)
            db.session.delete(product)
            db.session.commit()
            print('Product Deleted')


# Inner classes for error handling
class ServiceEventHandler(ReconnectionListener, ReconnectionAttemptListener, ServiceInterruptionListener):
    def on_reconnected(self, e: ServiceEvent):
        print("\non_reconnected")
        print(f"Error cause: {e.get_cause()}")
        print(f"Message: {e.get_message()}")

    def on_reconnecting(self, e: "ServiceEvent"):
        print("\non_reconnecting")
        print(f"Error cause: {e.get_cause()}")
        print(f"Message: {e.get_message()}")

    def on_service_interrupted(self, e: "ServiceEvent"):
        print("\non_service_interrupted")
        print(f"Error cause: {e.get_cause()}")
        print(f"Message: {e.get_message()}")


def initialize_mqlistener():
    # Broker Config
    broker_props = {
        "solace.messaging.transport.host": os.environ.get(
            'SOLACE_HOST') or "tcps://SOLACE_HOST:55443",
        "solace.messaging.service.vpn-name": os.environ.get('SOLACE_VPN') or "mediastore-spoke-service",
        "solace.messaging.authentication.scheme.basic.username": os.environ.get(
            'SOLACE_USERNAME') or "solace-cloud-client",
        "solace.messaging.authentication.scheme.basic.password": os.environ.get(
            'SOLACE_PASSWORD') or "5mefqtd8ikk1cqr94vrpv6bqm9"
    }

    transport_security_strategy = TLS.create().without_certificate_validation()

    messaging_service = MessagingService.builder().from_properties(broker_props) \
        .with_reconnection_retry_strategy(RetryStrategy.parametrized_retry(20, 3)) \
        .with_transport_security_strategy(transport_security_strategy) \
        .build()
    # .with_authentication_strategy(authentication_strategy)\

    # Blocking connect thread
    messaging_service.connect()
    print(f'Messaging Service connected? {messaging_service.is_connected}')

    # Event Handling for the messaging service
    service_handler = ServiceEventHandler()
    messaging_service.add_reconnection_listener(service_handler)
    messaging_service.add_reconnection_attempt_listener(service_handler)
    messaging_service.add_service_interruption_listener(service_handler)

    # Queue name.
    # NOTE: This assumes that a persistent queue already exists on the broker with the right topic subscription
    # queue_name = "sample-queue"
    queue_name = "mediastore-local-q-main"
    durable_exclusive_queue = Queue.durable_exclusive_queue(queue_name)
    try:
        # Build a receiver and bind it to the durable exclusive queue
        persistent_receiver: PersistentMessageReceiver = messaging_service.create_persistent_message_receiver_builder() \
            .with_message_auto_acknowledgement() \
            .build(durable_exclusive_queue)
        persistent_receiver.start()

        # Callback for received messages
        persistent_receiver.receive_async(MessageHandlerImpl())
        print(f'PERSISTENT receiver started... Bound to Queue [{durable_exclusive_queue.get_name()}]')
        time.sleep(1)
    # Handle API exception
    except PubSubPlusClientError as exception:
        print(f'\nMake sure queue {queue_name} exists on broker!')

    # finally:
    #     if persistent_receiver and persistent_receiver.is_running():
    #         print('\nTerminating receiver')
    #         persistent_receiver.terminate(grace_period=0)
    #     print('\nDisconnecting Messaging Service')
    #     messaging_service.disconnect()


def initialize_mqlistener_remote_q():
    # Broker Config
    broker_props = {
        "solace.messaging.transport.host": os.environ.get(
            'SOLACE_HOST') or "tcps://SOLACE_HOST:55443",
        "solace.messaging.service.vpn-name": os.environ.get('SOLACE_VPN') or "mediastore-spoke-service",
        "solace.messaging.authentication.scheme.basic.username": os.environ.get(
            'SOLACE_USERNAME') or "solace-cloud-client",
        "solace.messaging.authentication.scheme.basic.password": os.environ.get(
            'SOLACE_PASSWORD') or "5mefqtd8ikk1cqr94vrpv6bqm9"
    }

    transport_security_strategy = TLS.create().without_certificate_validation()

    messaging_service = MessagingService.builder().from_properties(broker_props) \
        .with_reconnection_retry_strategy(RetryStrategy.parametrized_retry(20, 3)) \
        .with_transport_security_strategy(transport_security_strategy) \
        .build()
    # .with_authentication_strategy(authentication_strategy)\

    # Blocking connect thread
    messaging_service.connect()
    print(f'Messaging Service connected? {messaging_service.is_connected}')

    # Event Handling for the messaging service
    service_handler = ServiceEventHandler()
    messaging_service.add_reconnection_listener(service_handler)
    messaging_service.add_reconnection_attempt_listener(service_handler)
    messaging_service.add_service_interruption_listener(service_handler)

    # Queue name.
    # NOTE: This assumes that a persistent queue already exists on the broker with the right topic subscription
    # queue_name = "sample-queue"
    queue_name = "mediastore-remote-q-main"
    durable_exclusive_queue = Queue.durable_exclusive_queue(queue_name)
    try:
        # Build a receiver and bind it to the durable exclusive queue
        persistent_receiver: PersistentMessageReceiver = messaging_service.create_persistent_message_receiver_builder() \
            .with_message_auto_acknowledgement() \
            .build(durable_exclusive_queue)
        persistent_receiver.start()

        # Callback for received messages
        persistent_receiver.receive_async(MessageHandlerImpl())
        print(f'PERSISTENT receiver started... Bound to Queue [{durable_exclusive_queue.get_name()}]')
        time.sleep(1)
    # Handle API exception
    except PubSubPlusClientError as exception:
        print(f'\nMake sure queue {queue_name} exists on broker!')

    # finally:
    #     if persistent_receiver and persistent_receiver.is_running():
    #         print('\nTerminating receiver')
    #         persistent_receiver.terminate(grace_period=0)
    #     print('\nDisconnecting Messaging Service')
    #     messaging_service.disconnect()


if __name__ == '__main__':
    print("Starting the main method...")
    initialize_mqlistener()
    initialize_mqlistener_remote_q()

