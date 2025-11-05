import requests
import json


def test_slack_webhook(webhook_url):
    """Test Slack webhook with a sample message"""
    if not webhook_url:
        print("Error: Please add your webhook URL first!")
        return

    message = {
        "text": "🎉 Test message from Inventory Alert System",
        "attachments": [
            {
                "color": "#36a64f",
                "title": "Test Alert",
                "text": "If you can see this message, your Slack integration is working correctly!",
                "fields": [{"title": "Status", "value": "Connected", "short": True}],
            }
        ],
    }

    try:
        response = requests.post(webhook_url, json=message)
        if response.status_code == 200:
            print("✅ Success! Check your Slack channel for the test message.")
        else:
            print(f"❌ Error: Received status code {response.status_code}")
            print(f"Response: {response.text}")
    except Exception as e:
        print(f"❌ Error sending message: {str(e)}")


if __name__ == "__main__":
    # Get the webhook URL from consumer.py
    try:
        from consumer import SLACK_CONFIG

        webhook_url = SLACK_CONFIG["webhook_url"]
        test_slack_webhook(webhook_url)
    except ImportError:
        print("Error: Couldn't import SLACK_CONFIG from consumer.py")
    except Exception as e:
        print(f"Error: {str(e)}")
