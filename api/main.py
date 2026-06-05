from fastapi import FastAPI
import pika
import json

app = FastAPI()

@app.post("/send-notification/")
def send_notification(message: str):
    # 1. Connect to RabbitMQ
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host='rabbitmq_broker') # 'rabbitmq_broker' is the name i gave in docker-compose.yml
    )
    channel = connection.channel()

    # 2. Create the Queue if it doesn't exist
    channel.queue_declare(queue='notificatio_queue', durable=True)

    # 3. Message content (package)
    payload = {
        "text": message,
        "status": "pending"
    }

    # 4. Send the package to the Queue
    channel.basic_publish(
        exchange='',
        routing_key='notification_queue', # Destination
        body=json.dumps(payload) # Convert to string
    )

    # 5. Close Connection 
    connection.close()

    return {"status": "Message sent successfully!", "your_message": message}
