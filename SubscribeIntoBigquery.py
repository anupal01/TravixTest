import json
from google.cloud import bigquery
from google.cloud import pubsub_v1
import yaml
import sys


def readconfig(configfile):
    with open(configfile) as file:
        values = yaml.load(file)
        return values


#configuration_file = 'SubscriberConfigFlight-Itinerary.yaml'
configuration_file = sys.argv[1]
attributes = readconfig(configuration_file)
service_account = attributes['Service_Account_File']
project_id = attributes['Project_id']
subscription_name = attributes['Subscription_name']
bigquery_table = attributes['Bigquery_table']
timeout = attributes['Timeout']


subscriber = pubsub_v1.SubscriberClient.from_service_account_file(service_account)
subscription_path = subscriber.subscription_path(
    project_id, subscription_name
)
bigquery_client = bigquery.Client.from_service_account_json(service_account)
objtable = bigquery_client.get_table(bigquery_table)


def callback(message):
    data = json.loads(message.data.decode('utf8').replace("'", '"'))
    query_job = bigquery_client.insert_rows_json(objtable, json_rows=[data], row_ids=None)

    if message.attributes:
        print("Attributes:")
        for key in message.attributes:
            value = message.attributes.get(key)
            print("{}: {}".format(key, value))

    message.ack()


read_subscribed_future = subscriber.subscribe(
    subscription_path, callback=callback
)
print("Listening for messages on {}..\n".format(subscription_path))

# result() in a future will block indefinitely if `timeout` is not set,
# unless an exception is encountered first.
try:
    read_subscribed_future.result()
except:  # noqa
    read_subscribed_future.cancel()



