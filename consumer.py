# # consumer_print.py
# from kafka import KafkaConsumer
# import json

# # Kafka consumer
# consumer = KafkaConsumer(
#     'inventory-alerts',
#     bootstrap_servers='localhost:9092',
#     value_deserializer=lambda m: json.loads(m.decode('utf-8')),
#     auto_offset_reset='earliest',
#     enable_auto_commit=True
# )

# for msg in consumer:
#     alert = msg.value
#     item_name = alert['item_name']
#     quantity = alert['quantity']

#     # Print alert to terminal instead of Slack
#     if quantity < 10:
#         print(f"⚠️ ALERT: Quantity of {item_name} is low ({quantity})!")
#     else:
#         print(f"{item_name} quantity is OK ({quantity})")
# consumer_with_slack.py
# consumer_with_slack.py
from kafka import KafkaConsumer
import json
import requests

# Replace with your Slack webhook URL
slack_webhook_url = "https://hooks.slack.com/services/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"

# Kafka consumer
consumer = KafkaConsumer(
    'inventory-alerts',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='earliest',
    enable_auto_commit=True
)

print("Consumer started, waiting for alerts...")

for msg in consumer:
    alert = msg.value
    item_name = alert['item_name']
    quantity = alert['quantity']

    if quantity < 10:
        print(f"⚠️ ALERT: Quantity of {item_name} is low ({quantity})!")

        # Prepare payload
        payload = {
            "text": f"⚠️ ALERT: Quantity of {item_name} is low ({quantity})!"
        }

        # Send Slack message
        response = requests.post(slack_webhook_url, json=payload)

        # Check response
        if response.status_code == 200:
            print("Message sent successfully to Slack!")
        else:
            print(f"Failed to send message. Status code: {response.status_code}, Response: {response.text}")
    else:
        print(f"{item_name} quantity is OK ({quantity})")
