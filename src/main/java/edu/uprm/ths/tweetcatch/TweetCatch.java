package edu.uprm.ths.tweetcatch;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;

/**
 * Created by manuel on 2/18/17.
 */
public class TweetCatch {

    public static void main(String[] args) throws Exception {
        // Start the Logger
        Logger logger = LogManager.getRootLogger();
        logger.trace("Starting application.");
        String topic = "trump";
        logger.trace("Topic: " + topic);
        ConfigurationBuilder cb = new ConfigurationBuilder();
        cb.setDebugEnabled(true)
                .setJSONStoreEnabled(true)
                .setOAuthConsumerKey("xSL5FtNNrXqYtj0qyLBAoEHJF")
                .setOAuthConsumerSecret("MlAkJxyGxAfJZ28HZxXu8XSWZpVG8zDepkTYdjqyoArPpdRM22")
                .setOAuthAccessToken("81409457-zSbn3elxFuBregKhjQQ4l3quNzLEuaA9KnKGgZ23l")
                .setOAuthAccessTokenSecret("i7frmi03QOrr0XBZ2t7jOgsHfwCmvGwIV6hkXW36EuD49");

        // create the factory for streams
        logger.trace("Building Twitter Stream Factory.");
        TwitterStreamFactory factory = new TwitterStreamFactory(cb.build());
        TwitterStream twitterStream = factory.getInstance();
        logger.trace("Starting listener.");

        twitterStream.addListener(new StatusListener() {
            public void onStatus(Status status) {
                String text = status.getText();

                if (text.contains("Trump")) {
                    // write to kafka
                    String rawJson = TwitterObjectFactory.getRawJSON(status);
                    Producer<String,String> producer = TweetKafkaProducer.getInstance().getProducer();
                    producer.send(new ProducerRecord<String, String>("trump",status.getUser().getScreenName(), rawJson));
                }
            }

            public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {

            }

            public void onTrackLimitationNotice(int i) {

            }

            public void onScrubGeo(long l, long l1) {

            }

            public void onStallWarning(StallWarning stallWarning) {

            }

            public void onException(Exception e) {
                e.printStackTrace();
            }
        });
        logger.trace("Starting to track tweet data.");
        twitterStream.sample();
    }
}