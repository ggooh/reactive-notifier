import pika 
import time
import json

def process_notifications(ch, method, properties, body):
    # 1. Receive and convert 
    payload = json.loads(body)
    message = payload.get("text")

    print(f" [x] Received new notification request: {message}")

    time.sleep(2)

    # 2. Tell RabbitMQ to remove it when done   
    ch.basic_ack(delivery_tag=method.delivery_tag)

def start_worker():
    # Connect to the RabbitMQ broker
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host='rabbitmq_broker')
    )
    channel = connection.channel()

    channel.queue_declare(queue='notification_queue', durable=True)

    channel.basic_qos(prefetch_count=1)

    channel.basic_consume(queue='notification_queue', on_message_callback=process_notifications)

    print(' [*] Worker is online and waiting for messages. To exit press CTRL+C')
    channel.start_consuming()

if __name__ == '__main__':
    start_worker()