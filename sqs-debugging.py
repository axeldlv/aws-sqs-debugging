import boto3
import os
import json
from datetime import datetime

# Config
REGION = os.getenv("REGION", "eu-west-1")
SQS_ENDPOINT = os.getenv("SQS_ENDPOINT", "https://localhost.localstack.cloud:4566")

sqs = boto3.client(
    "sqs",
    region_name=REGION,
    endpoint_url=SQS_ENDPOINT,
    aws_access_key_id="test",
    aws_secret_access_key="test"
)

def human_time(timestamp_ms):
    return datetime.fromtimestamp(int(timestamp_ms) / 1000).strftime('%Y-%m-%d %H:%M:%S')

def list_messages():
    print("Checking messages in queue...\n")    
    selected_queue_url = list_queues_and_choose()
    
    response = sqs.receive_message(
        QueueUrl=selected_queue_url,
        AttributeNames=['All'],
        MessageAttributeNames=['All'],
        MaxNumberOfMessages=10,
        WaitTimeSeconds=2,
        VisibilityTimeout=0
    )

    messages = response.get("Messages", [])
    if not messages:
        print("No messages available in the queue.")
        return

    for idx, msg in enumerate(messages, 1):
        print(f"\n--- Message #{idx} ---")
        print(f"MessageId: {msg['MessageId']}")
        print(f"ReceiptHandle: {msg['ReceiptHandle']}")

        # SQS attributes
        attrs = msg.get("Attributes", {})
        print(f"SentTimestamp: {human_time(attrs.get('SentTimestamp', '0'))}")
        print(f"ReceiveCount: {attrs.get('ApproximateReceiveCount')}")
        print(f"MessageGroupId: {attrs.get('MessageGroupId')}")
        print(f"MessageDeduplicationId: {attrs.get('MessageDeduplicationId')}")
        print(f"SequenceNumber: {attrs.get('SequenceNumber')}")
        print(f"VisibilityTimeout Active (IsVisible): {'false' if int(attrs.get('ApproximateReceiveCount', 0)) > 0 else 'true'}")

        # Raw Body
        body = msg["Body"]
        print(f"\nRaw Body:\n{body}")

def purge_queue():
    print("Which queue do you want to purge: \n")
    selected_queue_url = list_queues_and_choose()
    if selected_queue_url:
        confirm = input("Are you sure you want to purge the queue? This deletes ALL messages. (yes/no): ")
        if confirm.lower() == "yes":
            sqs.purge_queue(QueueUrl=selected_queue_url)
            print("Queue purged.")
        else:
            print("Purge cancelled.")

def list_queues():
    list = sqs.list_queues()
    print(json.dumps(list, indent=4))
    
def list_queues_and_choose():
    response = sqs.list_queues()
    queue_urls = response.get('QueueUrls', [])
    
    if not queue_urls:
        print("No queues found.")
        return None
    
    print("Choose a queue: \n")
    for idx, url in enumerate(queue_urls, start=1):
        print(f"{idx}. {url}")

    # Get user's choice
    while True:
        try:
            choice = int(input("\n Enter the number of the queue you want to select: "))
            if 1 <= choice <= len(queue_urls):
                selected_queue = queue_urls[choice - 1]
                print(f"You selected: {selected_queue} \n")
                return selected_queue
            else:
                print(f"Please enter a number between 1 and {len(queue_urls)}. \n")
        except ValueError:
            print("Invalid input. Please enter a number. \n")    

if __name__ == "__main__":
    print("SQS Debug Utility")
    print("=================\n")
    print("1. List messages in queue")
    print("2. Purge queue")
    print("3. List All SQS")    
    choice = input("Choose an action (1, 2 or 3): ")

    if choice == "1":
        list_messages()
    elif choice == "2":
        purge_queue()
    elif choice == "3":
        list_queues()        
        
    else:
        print("Invalid choice.")
