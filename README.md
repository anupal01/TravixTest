# TravixTest
 This is a single repo with implementation to read from gcs and publish into pubsub,it also has the implementation to read from by join two files transaction.json and locations.json to create a new structure and insert into bigquery from pubsub. The repo also contains the python implemantation to read from pubsub and push to bigquery.

 Use cases:
 1.Read from gcs and pusblish to PubSub. Below are the scripts and driver.
    PublisherApp.py -- main script
    PublisherConfigAirport-location.yaml --Config file specifying the bucket location, filename, and the PubSub topic to push this data. This is the config to push the Location master data.
    PublisherConfigFlight-Itinerary.yaml --Config file specifying the bucket location, filename, and the PubSub topic to push this data. This is the config to push the Flight transaction data.

2.Read the location master and flight transaction detail file from GCS. It does the below transformation on the data.
    Load the location master to a panda dataframe
    Load the flight transaction, split the itinerary and drive the country and region for each airport, per transaction.The derived element is added back to the main transaction under the tags: Trip_type and CountryList
    Trip_type : can be either International or Domestic.
    CountryList: Airport Code, Country, Region.
    PublisherApp.py -- main script.
    TransformDataConfig.yaml -- Config file.

3.Read from PubSub and insert into Bigquery. Below are the scripts and driver.
    SubscribeIntoBigquery.py --main script
    Configuration Files:
        SubscriberConfigAirportLocation.yaml
        SubscriberConfigFlight-Itinerary.yaml
        SubscriberConfigTrip-Detail.yaml


    