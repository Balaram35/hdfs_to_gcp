import base64
import json
from google.cloud import pubsub_v1
from google.cloud import bigquery
from datetime import datetime

# Initialize Pub/Sub client and BigQuery client
publisher = pubsub_v1.PublisherClient()
bq_client = bigquery.Client()
TOPIC_NAME = 'projects/gcp-to-bigquery-schemav/topics/gcs_to_pubsub'
dataset_id = 'gcp-to-bigquery-schemav.gcs_dataset'
table_id = 'gcs_files'

def gcs_to_pubsub(event, context):
    """Triggered by a change in a GCS bucket.
    Args:
        event (dict): The Pub/Sub message payload.
        context (google.cloud.functions.Context): Metadata of the event.
    """
    # Extract relevant fields from the event
    bucket_name = event.get('bucket')
    file_name = event.get('name')
    generation = event.get('generation')
    time_created = event.get('timeCreated')
    updated = event.get('updated')
    
    # Create event data
    event_data = {
        'bucket': bucket_name,
        'name': file_name,
        'eventType': context.event_type,
        'timestamp': datetime.utcnow().isoformat(),  # Current UTC timestamp
        'time_created': time_created,
        'updated': updated
    }

    # Check if timeCreated and updated are the same
    if time_created == updated:
        # Create the message payload
        message = {
            'bucket': bucket_name,
            'file_name': file_name,
            'generation': generation,
            'time_created': time_created,
            'updated': updated
        }
        
        # Convert message to JSON
        message_json = json.dumps(message).encode('utf-8')
        
        # Publish message to Pub/Sub
        future = publisher.publish(TOPIC_NAME, message_json)
        print(f"Published message ID: {future.result()}")

        # Insert data into BigQuery
        rows_to_insert = [event_data]
        errors = bq_client.insert_rows_json(f"{dataset_id}.{table_id}", rows_to_insert)
    
        if errors:
           print(f"Encountered errors while inserting rows: {errors}")
        else:
           print("Successfully inserted rows into BigQuery")
    else:
        print(f"File {file_name} in bucket {bucket_name} was updated, not created. No message sent to Pub/Sub.")
