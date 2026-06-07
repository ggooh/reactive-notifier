from fastapi import FastAPI
from pydantic import BaseModel
import pika
import json

app = FastAPI()

# 1. Create a Schema for order data
class OrderNotification(BaseModel):
    email: str
    customer_name: str
    order_id: str
    amount: float

@app.post("/send-notification/")
def send_notification(order: OrderNotification):
    # 2. Connect to RabbitMQ
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host='rabbitmq_broker') # 'rabbitmq_broker' is the name i gave in docker-compose.yml
    )
    channel = connection.channel()

    # 3. Create the Queue if it doesn't exist
    channel.queue_declare(queue='notificatio_queue', durable=True)

    # 4. Convert the Python object into a Dictionary, and into a JSON string
    payload = order.model_dump()

    # 5. Send the JSON to RabbitMQ
    channel.basic_publish(
        exchange='',
        routing_key='notification_queue', # Destination
        body=json.dumps(payload) # Convert to string
    )

    # 6. Close Connection 
    connection.close()

    return {"status": "Notification data forwarded to queue successfully!", "sent_to": order.email}
