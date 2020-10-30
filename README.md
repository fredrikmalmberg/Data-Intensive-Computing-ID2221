# ID2221
Group project during studies at KTH.

We built a combined Scala/Python application that integrated to Twitter, analysed tweets and made predictions of potentially viral tweets using a continuosly trained model built  in Tensorflow. The result is visualised in a basic web app. 
---------
Data is at first received through a streaming connection from Twitter using the Filtered Stream API. The json content is directly sent to kafka as it is received.

Spark Streaming is used to listen to the kafka topics and process the json content of the messages as they come. The processed data is then written in two different cassandra tables.

Incoming tweets are batched up by up to 100 by 20s window (because of API limitations) to get metrics for each tweet every minute for 5 minute as training data for the machine learning model. 

A final lookup of their metrics is done 30 minutes after initially collecting the tweet to find out the ground truth of our targeted value. Each lookup result is sent to kafka to be processed in Spark Streaming and written to cassandra.

A cassandra table stores all of the tweets with relevant information for visualization purposes as well as the metrics used as training input and target for the machine learning model. The machine learning model reads periodically from this table to predict values for the input values without target and is trained for those with target values.

A visualization script reads from the final cassandra table to display relevant values about the gathered tweets and the predictions made by the machine learning model.
