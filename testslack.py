
# slack_test.py
import requests

# Replace with your Slack webhook URL
slack_webhook_url = "https://hooks.slack.com/services/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"

# Payload with the message
payload = {
    "text": "Hi from Python! 👋"
}

# Send the message
response = requests.post(slack_webhook_url, json=payload)

# Check if it worked
if response.status_code == 200:
    print("Message sent successfully!")
else:
    print(f"Failed to send message. Status code: {response.status_code}, Response: {response.text}")
