# producer_db.py
import psycopg2
from kafka import KafkaProducer
import json

# PostgreSQL connection
conn = psycopg2.connect(
    host="localhost",
    database="your_db",
    user="your_user",
    password="your_password"
)
cursor = conn.cursor()

# Kafka producer for alerts
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Ensure table exists
cursor.execute("""
CREATE TABLE IF NOT EXISTS inventory (
    item_id SERIAL PRIMARY KEY,
    item_name VARCHAR(50) UNIQUE,
    quantity INT
)
""")
conn.commit()

while True:
    item_name = input("Enter item name: ")
    action = input("Enter action (buy/sell): ").lower()
    if action not in ['buy', 'sell']:
        print("Invalid action!")
        continue
    try:
        qty = int(input("Enter quantity: "))
    except ValueError:
        print("Quantity must be an integer!")
        continue

    # Check current quantity
    cursor.execute("SELECT quantity FROM inventory WHERE item_name=%s", (item_name,))
    result = cursor.fetchone()
    if result:
        current_qty = result[0]
        new_qty = current_qty + qty if action == 'buy' else current_qty - qty
        cursor.execute("UPDATE inventory SET quantity=%s WHERE item_name=%s", (new_qty, item_name))
    else:
        new_qty = qty if action == 'buy' else 0
        cursor.execute("INSERT INTO inventory (item_name, quantity) VALUES (%s, %s)", (item_name, new_qty))

    conn.commit()
    print(f"{action.upper()} {qty} {item_name}, new quantity: {new_qty}")

    # Only send alert to Kafka if quantity < 10
    if new_qty < 10:
        alert_msg = {"item_name": item_name, "quantity": new_qty}
        producer.send('inventory-alerts', value=alert_msg)
        print(f"Alert sent to Kafka: {alert_msg}")
