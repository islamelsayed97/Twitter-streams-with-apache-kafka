# Twitter streams with apache kafka
I used Twittwe to get real time tweets streams into an application that will put the data into Apache Kafka, then these tweets end up in MongoDB collection at the end.

### The project consists of two main parts:
- **Kafka Producer:** the role of this part is to get the tweets from twitter using twitter API, then put these tweets into Apache kafka.
- **Kafka Consumer:** the consumer first gets the tweets from apache kafka and store it in MongoDB database.


### Tools and technologies used in this project:
Java, Apache Kafka, MongoDB and Twitter API.
