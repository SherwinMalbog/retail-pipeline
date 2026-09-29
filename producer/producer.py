from confluent_kafka import Producer
from faker import Faker
from datetime import datetime, timedelta
import json
import random

config = {
    "bootstrap.servers": "localhost:19092"
}

fake = Faker()
producer = Producer(config)

base_time = datetime.now()
countries = ["US", "SG", "JP", "AU"]


for i in range(1,6):
    cust_number = fake.random_int(min=1, max=100)
    amount = random.uniform(100, 5000)
    billing_country = "PH"
    shipping_country = "PH" 
    event_time = base_time + timedelta(minutes=i)
    chance = random.random()
    
    if chance < 0.25:
        amount = random.uniform(5000, 50000)
    elif chance < 0.35:
        shipping_country = random.choice(countries)
    elif chance < 0.40:
        amount = random.uniform(50000, 100000)

    if i == 1:
        cust_number = 28
    elif i == 2:
        cust_number = 28
    elif i == 4:
        shipping_country = "US"
    elif i == 5:
        amount = 50000
        

    message = {
        "order_id": f"ORD-{i:03d}",
        "customer_id": f"CUST-{cust_number:03d}",
        "amount": round(amount, 2),
        "billing_country": billing_country,
        "shipping_country": shipping_country,
        "timestamp": event_time.isoformat()
    }

    json_message = json.dumps(message)


    producer.produce("orders", json_message)

producer.flush()

print("Producer created successfully")