import pika 
import time
import json
import smtplib
import os
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
SMTP_SERVER = os.getenv("SMTP_SERVER", "localhost")
SMTP_PORT = int(os.getenv("SMTP_PORT", 1025))
SENDER_EMAIL = os.getenv("SENDER_EMAIL", "no-reply@example.com")

def send_html_email(target_email, customer_name, order_id, amount):
    msg = MIMEMultipart('alternative')
    msg['Subject'] = f"Order Confirmation #{order_id}"
    msg['From'] = SENDER_EMAIL
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

    with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
        server.sendmail(msg['From'], [msg['To']], msg.as_string())    
                        

def process_notifications(ch, method, properties, body):
    # 1. Decode and analyze the incoming message body
    try:
        
        order_data = json.loads(body)
        target_email = order_data.get("email")
        customer = order_data.get("customer_name")
        order_id = order_data.get("order_id")
        amount = order_data.get("amount")

        print(f" [x] Receiving payload for processing...")

        # Payload validation gateway
        if not target_email or "@" not in target_email:
            raise ValueError(f"Validation Failed: Missing or malformed email ('{target_email}').")

        if not order_id or str(order_id).strip().lower() == "none":
            raise ValueError("Validation Failed: 'order_id' is missing or invalid.")
        
        if amount is None:
            raise ValueError("Validation Failed: 'amount' field is strictly required.")
        
        try:
            if float(amount) <= 0:
                raise ValueError("Validation Failed: 'amount' must be greater than zero.")
        except TypeError:
            raise ValueError(f"Validation Failed: 'amount' must be a valid number, received '{type(amount).__name__}'.")
    
        if not customer or str(customer).strip().lower() == "none":
            print(" [!] Warning: 'customer_name' missing. Falling back to default greeting.")
            customer = "Valued Customer"

        print(f" [x] Payload verified. Processing order #{order_id} for {customer}")
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
                pika.ConnectionParameters(host=RABBITMQ_HOST)
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

    channel.queue_bind(exchange='dlx_exchange', queue='dlq_queue', routing_key='dead_letter_key')

    main_queue_arguments = {'x-dead-letter-exchange': 'dlx_exchange', 'x-dead-letter-routing-key': 'dead_letter_key'}

    channel.queue_declare(queue='notification_queue', durable=True, arguments=main_queue_arguments)

    channel.basic_qos(prefetch_count=1)

    channel.basic_consume(queue='notification_queue', on_message_callback=process_notifications)

    print(' [*] Worker is online and waiting for messages. To exit press CTRL+C')
    channel.start_consuming()

if __name__ == '__main__':
    start_worker()