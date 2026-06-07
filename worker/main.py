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
        return
    
    final_html = html_content.format(
        customer_name=customer_name,
        order_id=order_id,
        amount=amount
    )

    msg.attach(MIMEText(final_html, 'html'))

    with smtplib.SMTP(smtp_server, smtp_port) as server:
        server.sendmail(msg['From'], [msg['To']], msg.as_string())    
                        

def process_notifications(ch, method, properties, body):
    # 1. Unwrap the JSON package from RabbitMQ into Python dictionary
    order_data = json.loads(body)

   # 2. Extract the specific labels from the package
    target_email = order_data.get("email")
    customer = order_data.get("customer_name")
    order_id = order_data.get("order_id")
    amount = order_data.get("amount")

    print(f" [x] Processing order #{order_id} for {customer}")

    try:
        send_html_email(target_email, customer, order_id, amount)
        print(f" [x] Email successfully sent to {target_email}")
    except Exception as e:
        print(f" [!] Failed to send email: {str(e)}")

    print("-" * 40)

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