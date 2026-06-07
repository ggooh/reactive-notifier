import pika 
import time
import json

def process_notifications(ch, method, properties, body):
    # 1. Unwrap the JSON package from RabbitMQ into Python dictionary
    order_data = json.loads(body)

   # 2. Extract the specific labels from the package
    target_email = order_data.get("email")
    customer = order_data.get("customer_name")
    order_id = order_data.get("order_id")
    amount = order_data.get("amount")

    print(f" [x] New Order Received! ID: {order_id}")
    print(f" [x] Preparing email for: {customer} ({target_email})")
    print(f" [x] Total amount: {amount} EUR")

    time.sleep(2)

    print(f" [x] Email successfully 'sent' to {target_email}!\n" + "-"*40)

    # 3. Tell RabbitMQ to remove it when done   
    ch.basic_ack(delivery_tag=method.delivery_tag)

def start_worker():
    # Retry mechanism to try to connect to RabbitMQ
    max_retries = 5
    for attempt in range(max_retries):
        try:
            print(f" [*] Attempting to connect to RabbitMQ (Attempt {attempt + 1}/{max_retries})...")
            connection = pika.BlockingConnection(
                pika.ConnectionParameters(host='rabbitmq_broker')
            )
            break 
        except pika.exceptions.AMQPConnectionError:
            print(" [!] RabbitMQ is not ready yet. Waiting 5 seconds...")
            time.sleep(5)
    else:
        print(" [x] Failed to connect to RabbitMQ. Shutting down worker.")
        return
    
    channel = connection.channel()

    channel.queue_declare(queue='notification_queue', durable=True)

    channel.basic_qos(prefetch_count=1)

    channel.basic_consume(queue='notification_queue', on_message_callback=process_notifications)

    print(' [*] Worker is online and waiting for messages. To exit press CTRL+C')
    channel.start_consuming()

if __name__ == '__main__':
    start_worker()