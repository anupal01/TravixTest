from google.cloud import storage
from google.cloud import pubsub_v1
import ndjson
import concurrent.futures as futures
import yaml
import sys
import pandas
import pandasql

#configuration_file = 'TransformDataConfig.yaml'
configuration_file = sys.argv[1]
json_data = ''
list = []
futures = dict()
dataframe = None


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


def write_data(dataset, topicname):
    for eachline in dataset:
        msg = str(eachline)
        futures.update({msg: None})
        # When you publish a message, the client returns a future.
        future = publisher.publish(
            topicname, msg.encode("utf-8")  # data must be a bytestring.
        )
        futures[msg] = future
        # Publish failures shall be handled in the callback function.
        future.add_done_callback(get_callback(future, msg))


def read_write_file(bucket_name, filename,topicname):
    bucket = storage_client.get_bucket(bucket_name)  # get bucket data as blob
    blob = bucket.get_blob(filename)  # convert to string
    data = ndjson.loads(blob.download_as_string())
    for eachline in data:
        df = pandas.DataFrame(eachline["Itinerary"].split("-"))
        df.columns = ['AirportCode']
        df_inner = pandas.merge(df, dataframe, on='AirportCode', how='inner')
        x = df_inner['CountryName'].unique()
        trip_type = 'International' if x.size > 1 else 'Domestic'
        y = df_inner.to_dict('r')
        eachline['CountryList'] = y
        eachline['Trip_type'] = trip_type
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
    data = ndjson.loads(blob.download_as_string())
    df = pandas.DataFrame(data)
    return df

attributes = readconfig(configuration_file)
service_account = attributes['Service_Account_File']

# This should be reapled such that it uses the Environment Variable from the cluster.

storage_client = storage.Client.from_service_account_json(service_account)
publisher = pubsub_v1.PublisherClient.from_service_account_file(service_account)
#
bucket_name = attributes['Bucket_Name']
dimension_File = attributes['Dimension_File']
fact_File = attributes['Fact_File']
topic = attributes['Topic']

#dimension_File = read_file(bucket_name,dimension_File)
fact_data = read_file(bucket_name,fact_File)
dataframe = fact_data
dimension_File = read_write_file(bucket_name,dimension_File,topic)



