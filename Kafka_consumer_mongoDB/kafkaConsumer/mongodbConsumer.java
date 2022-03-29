package kafkaConsumer;

import com.google.gson.JsonParser;
import com.mongodb.MongoWriteException;
import com.mongodb.client.*;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class mongodbConsumer {

    public static KafkaConsumer<String, String> createConsumer(){

        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "Kafka_twitter_elasticsearch";
        String topic = "twitter_tweets";

        // properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // create the consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        // subscribe to topic
        consumer.subscribe(Collections.singleton(topic));

        return consumer;
    }

    private static JsonParser jsonParser = new JsonParser();
    public static String extractIdFromTweet(String tweetJson){
        // get the tweet and parse it as json format to extract id_str from it, then return it as string
        return jsonParser.parse(tweetJson)
                .getAsJsonObject()
                .get("id_str")
                .getAsString();
    }

    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(mongodbConsumer.class.getName());
        // create mongodb client
		// Note: You have to replace <password> in the next line with your password.
        MongoClient client = MongoClients
                .create("mongodb+srv://testUser:<password>@cluster0.v2s2k.mongodb.net/myFirstDatabase?retryWrites=true&w=majority");

        // get twitter database
        MongoDatabase db = client.getDatabase("Twitter");

        // get tweets database
        MongoCollection col = db.getCollection("Tweets");

        // create the kafka consumer
        KafkaConsumer<String, String> consumer = createConsumer();

        // poll for the data
        while(true) {

            // get the records
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            logger.info("we received "+records.count()+" records");

            // loop for each record to send it to the database
            for(ConsumerRecord<String, String> record : records){

                // get the id from the tweet to make it the document _id in MongoDB
                String id = extractIdFromTweet(record.value());

                // parse the record data and add the _id to it
                logger.info("Parsing the record...");
                String data = record.value();
                Document document = Document.parse(data).append("_id", id);

                // send the record. Here we may receive an error due to id duplication,
                // so we need to put it in try & catch to handle this exception
                try{
                    col.insertOne(document);
                }catch (MongoWriteException e){
                    logger.info("Skipped duplicated record");
                }
            }
        }
    }
}
