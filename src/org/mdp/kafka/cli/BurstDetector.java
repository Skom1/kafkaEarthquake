package org.mdp.kafka.cli;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Collections;
import java.util.LinkedList;
import java.util.Properties;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.mdp.kafka.def.KafkaConstants;

public class BurstDetector {
    public static final int FIFO_SIZE = 50;

    public static final int CRITICAL_WINDOW_SIZE = 50000;

    public static void main(String[] paramArrayOfString) throws FileNotFoundException, IOException {
        if (paramArrayOfString.length == 0 || paramArrayOfString.length > 2 || (paramArrayOfString.length == 2 && !paramArrayOfString[1].equals("replay"))) {
            System.err.println("Usage [inputTopic] [replay]");
            return;
        }
        Properties properties = KafkaConstants.PROPS;
        if (paramArrayOfString.length == 2) {
            properties.put("group.id", UUID.randomUUID().toString());
            properties.put("auto.offset.reset", "earliest");
        }
        KafkaConsumer kafkaConsumer = new KafkaConsumer(properties);
        kafkaConsumer.subscribe(Collections.singletonList(paramArrayOfString[0]));
        LinkedList<Object> linkedList = new LinkedList();
        byte a = 0;
        byte b = 0;
        byte c = 0;
        boolean bool1 = false;
        boolean bool2 = false;
        try {
            while (c < '@') {
            ConsumerRecords consumerRecords = kafkaConsumer.poll(Duration.ofMillis(10L));
            if (consumerRecords.isEmpty())
                c++;
            for (ConsumerRecord consumerRecord : consumerRecords) {
                linkedList.add(consumerRecord.value());
                if (linkedList.size() == 50) {
                    String[] arrayOfString1 = ((String)linkedList.getFirst()).split("\t");
                    String[] arrayOfString2 = ((String)linkedList.getLast()).split("\t");
                    SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    long l = Math.abs(simpleDateFormat.parse(arrayOfString2[0]).getTime() - simpleDateFormat
                            .parse(arrayOfString1[0]).getTime()) / 1000L;
                    if (l <= 25L && !bool1) {
                        bool1 = true;
                        a++;
                        System.out.println("RMayor:\n" + arrayOfString1[0] + "\t" + arrayOfString1[1] + "\tNRT: " + arrayOfString1[7] + "\t" + arrayOfString1[6]);
                    } else if (l <= 50L && !bool2) {
                        bool1 = false;
                        bool2 = true;
                        b++;
                        System.out.println("RMenor:\n" + arrayOfString1[0] + "\t" + arrayOfString1[1] + "\tNRT: " + arrayOfString1[7] + "\t" + arrayOfString1[6]);
                    } else if (l > 50L && bool2) {
                        bool2 = false;
                    }
                    linkedList.removeFirst();
                }
                c++;
            }
        }
    } catch (ParseException parseException) {
        parseException.printStackTrace();
    } finally {
        System.out.println("Rafagas mayores: " + a);
        System.out.println("Rafagas menores: " + b);
        kafkaConsumer.close();
    }
}
}