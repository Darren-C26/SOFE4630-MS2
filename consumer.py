from concurrent.futures import TimeoutError
from google.cloud import pubsub_v1

project_id = "regal-fortress-413306"
subscription_id = "smart_meter_clean-sub"
#Number of seconds the subscriber should listen for messages
timeout = 100.0

credentials_path = "cred.json"
subscriber = pubsub_v1.SubscriberClient.from_service_account_file(credentials_path)

subscription_path = subscriber.subscription_path(project_id, subscription_id)

def callback(message: pubsub_v1.subscriber.message.Message) -> None:
    try:
        # Check for Pub/Sub message
        record_value = message.data.decode("utf-8")
        print(f"Consumed record with value: {record_value}")

    except Exception as e:
        print(f"Error processing message: {e}")
    finally:
        message.ack()

streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
print(f"Listening for messages on {subscription_path}..\n")

# Wrap subscriber in a 'with' block to automatically call close() when done.
with subscriber:
    try:
        # When `timeout` is not set, result() will block indefinitely,
        # unless an exception is encountered first.
        streaming_pull_future.result(timeout=timeout)
    except TimeoutError:
        streaming_pull_future.cancel()  # Trigger the shutdown.
        streaming_pull_future.result()  # Block until the shutdown is complete.

