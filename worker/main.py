import pika 
import time
import json
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

def send_html_email(target_email, customer_name, order_id, amount):
    # 1. Setup SMTP connection parameters 
    smtp_server = "mailpit"
    smtp_port = 1025

    msg = MIMEMultipart('alternative')
    msg['Subject'] = f"Order Confirmation #{order_id}"
    msg['From'] = "store@reactive-notifier.com"
    msg['To'] = target_email
     
    # 2. Read the external HTML template file
    try:
        with open("email_template.html", "r", encoding="utf-8") as file:
            html_content = file.read()
    except FileNotFoundError:
        print(" [!] Error: email_template.html file missing!")
        raise
    
    final_html = html_content.format(
        customer_name=customer_name,
        order_id=order_id,
        amount=amount
    )

    msg.attach(MIMEText(final_html, 'html'))

    with smtplib.SMTP(smtp_server, smtp_port) as server:
        server.sendmail(msg['From'], [msg['To']], msg.as_string())    
                        

def process_notifications(ch, method, properties, body):
    # 1. Decode and analyze the incoming message body
    try:

        order_data = json.loads(body)
        target_email = order_data.get("email")
        customer = order_data.get("customer_name")
        order_id = order_data.get("order_id")
        amount = order_data.get("amount")

        print(f" [x] Processing order #{order_id} for {customer}")
    
        # 2. If the payload lacks a valid email, considered it a corrupted message.
        if not target_email:
            raise ValueError("Invalid payload: 'email' field is missing or empty")

        send_html_email(target_email, customer, order_id, amount)
        print(f" [x] Email successfully sent to {target_email}")

        # 3. Tell RabbitMQ to remove it when done   
        ch.basic_ack(delivery_tag=method.delivery_tag)

    except Exception as error:
        print(f" [!] CRITICAL ERROR during processing: {str(error)}")
        print(" [!] Rejecting message and routing to Dead Letter Queue (DLQ)...")

        # 4. Tell RabbitMQ to route it to the Dead Letter Exchange.
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
    
    print("-" * 40)


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

    # Declare the Dead Letter Exchange 
    channel.exchange_declare(exchange='dlx_exchange', exchange_type='direct')

    # Declare the Dead Letter Queue to store failed messages
    channel.queue_declare(queue='dlq_queue', durable=True)

    channel.queue_bind(
        exchange='dlx_exchange',
        queue='dlq_queue',
        routing_key='dead_letter_key'
    )

    main_queue_arguments = {
        'x-dead-letter-exchange': 'dlx_exchange',
        'x-dead-letter-routing-key': 'dead_letter_key'
    }

    channel.queue_declare(queue='notification_queue', durable=True, arguments=main_queue_arguments)

    channel.basic_qos(prefetch_count=1)

    channel.basic_consume(queue='notification_queue', on_message_callback=process_notifications)

    print(' [*] Worker is online and waiting for messages. To exit press CTRL+C')
    channel.start_consuming()

if __name__ == '__main__':
    start_worker()