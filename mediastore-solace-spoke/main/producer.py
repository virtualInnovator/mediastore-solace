# Guaranteed Publisher publishing persistent messages
import os
import platform
import time
import threading

from solace.messaging.messaging_service import MessagingService, ReconnectionListener, ReconnectionAttemptListener, \
    ServiceInterruptionListener, RetryStrategy, ServiceEvent
from solace.messaging.publisher.persistent_message_publisher import PersistentMessagePublisher
from solace.messaging.publisher.persistent_message_publisher import MessagePublishReceiptListener
from solace.messaging.resources.topic import Topic
from solace.messaging.config.transport_security_strategy import TLS

if platform.uname().system == 'Windows': os.environ["PYTHONUNBUFFERED"] = "1"  # Disable stdout buffer

lock = threading.Lock()  # lock object that is not owned by any thread. Used for synchronization and counting the

TOPIC_PREFIX = "solace/mediastore/local/"


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


class MessageReceiptListener(MessagePublishReceiptListener):
    def __init__(self):
        self._receipt_count = 0

    @property
    def receipt_count(self):
        return self._receipt_count

    def on_publish_receipt(self, publish_receipt: 'PublishReceipt'):
        with lock:
            self._receipt_count += 1
            print(f"\npublish_receipt:\n {self.receipt_count}\n")


def publish(method, body):
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
    # transport_security_strategy = TLS.create().with_certificate_validation(True, False, "./trusted-store")
    # authentication_strategy = ClientCertificateAuthentication.of("/path/to/certificate","/path/to/key","key_password")\
    #                           .with_certificate_and_key_pem("/path/to/pem/file")\
    #                           .with_private_key_password("private_key_password")

    # Build A messaging service with a reconnection strategy of 20 retries over an interval of 3 seconds
    # Note: The reconnections strategy could also be configured using the broker properties object
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

    # Create a persistent message publisher and start it
    publisher: PersistentMessagePublisher = messaging_service.create_persistent_message_publisher_builder().build()
    publisher.start_async()

    # set a message delivery listener to the publisher
    receipt_listener = MessageReceiptListener()
    publisher.set_message_publish_receipt_listener(receipt_listener)

    # Prepare the destination topic
    topic = Topic.of(TOPIC_PREFIX + f'admin')

    # Prepare outbound message payload and body
    message_body = body
    outbound_msg_builder = messaging_service.message_builder() \
        .with_application_message_id("mediastore_id") \
        .with_property("application", "mediastore") \
        .with_property("language", "Python") \
        .with_property("content-type", method)


    outbound_msg = outbound_msg_builder.build(f'{message_body}')

    publisher.publish(outbound_msg, topic)
    print(f'PERSISTENT publish message  is successful... Topic: [{topic.get_name()}]')
    time.sleep(1)
    print('\nTerminating Publisher')
    publisher.terminate()
    print('\nDisconnecting Messaging Service')
    messaging_service.disconnect()
