package org.mdp.kafka.cli;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Collections;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.mdp.kafka.def.KafkaConstants;

public class EarthquakeFilter {
    public static final String[] EARTHQUAKE_SUBSTRINGS = new String[] { "terremoto", "temblor", "sismo", "quake" };

    public static int TWEET_ID = 2;

    public static String tweetsTopic;

    public static String filteredTopic;

    public static void main(String[] paramArrayOfString) throws FileNotFoundException, IOException {
        if (paramArrayOfString.length != 2) {
            System.err.println("Usage [inpuTopic] [outputTopic]");
            return;
        }
        tweetsTopic = paramArrayOfString[0];
        KafkaConsumer kafkaConsumer = new KafkaConsumer(KafkaConstants.PROPS);
        kafkaConsumer.subscribe(Collections.singletonList(tweetsTopic));
        KafkaProducer kafkaProducer = new KafkaProducer(KafkaConstants.PROPS);
        filteredTopic = paramArrayOfString[1];
        try {
            while (true) {
                try {
                    ConsumerRecords consumerRecords = kafkaConsumer.poll(Duration.ofMillis(10L));
                    for (ConsumerRecord consumerRecord : consumerRecords) {
                        for (String str : EARTHQUAKE_SUBSTRINGS) {
                            if (((String)consumerRecord.value()).toLowerCase().contains(str)) {
                                String[] arrayOfString = ((String)consumerRecord.value()).split("\t");
                                kafkaProducer.send(new ProducerRecord(filteredTopic,
                                        Integer.valueOf(0),
                                        Long.valueOf((new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")).parse(arrayOfString[0]).getTime()), arrayOfString[TWEET_ID], consumerRecord
                                        .value()));
                                break;
                            }
                        }
                    }
                } catch (ParseException parseException) {
                    parseException.printStackTrace();
                    kafkaProducer.close();
                    break;
                }
            }
        } finally {
            kafkaProducer.close();
        }
    }
}
