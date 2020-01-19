from google.cloud import storage
from google.cloud import pubsub_v1
import ndjson
import concurrent.futures as futures
import yaml
import sys

#configuration_file = 'PublisherConfigFlight-Itinerary.yaml'
configuration_file = sys.argv[1]
json_data = ''
list = []
futures = dict()


def readconfig(configfile):
    with open(configfile) as file:
        values = yaml.load(file)
        return values


def get_callback(f, data):
    def callback(f):
        try:
            print(f.result())
            futures.pop(data)
        except:
            print("Please handle {} for {}.".format(f.exception(), data))

    return callback


def write_data(data, topicname):
    list = ndjson.loads(data)
    for eachline in list:
        msg = str(eachline)
        futures.update({msg: None})
        # When you publish a message, the client returns a future.
        future = publisher.publish(
            topicname, msg.encode("utf-8")  # data must be a bytestring.
        )
        futures[msg] = future
        # Publish failures shall be handled in the callback function.
        future.add_done_callback(get_callback(future, msg))


def read_file(bucket_name, filename):
    bucket = storage_client.get_bucket(bucket_name)  # get bucket data as blob
    blob = bucket.get_blob(filename)  # convert to string
    data = blob.download_as_string()
    return data


attributes = readconfig(configuration_file)
service_account = attributes['Service_Account_File']

# This should be reapled such that it uses the Environment Variable from the cluster.

storage_client = storage.Client.from_service_account_json(service_account)
publisher = pubsub_v1.PublisherClient.from_service_account_file(service_account)
#
files_topics = attributes['Files_Topics']
bucket_name = attributes['Bucket_Name']
for file, topic in files_topics.items():
    json_data = read_file(bucket_name, file)
    write_data(json_data, topic)
