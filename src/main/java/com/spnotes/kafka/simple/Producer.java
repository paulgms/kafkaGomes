package com.spnotes.kafka.simple;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Scanner;

/**
 * Created by Anik Paul Gomes on 12/28/15.
 */
public class Producer {
    private static Scanner in;
    public static void main(String[] argv)throws Exception {
        String[] messageArray = {"Go bearcats", "Northwest Missouri State University", "Google", "Stack overflow"};
        if (argv.length != 1) {
            System.err.println("Please specify 1 parameters ");
            System.exit(-1);
        }
        String topicName = argv[0];
        in = new Scanner(System.in);
        System.out.println("Enter message(type exit to quit)");

        //Configure the Producer
        Properties configProperties = new Properties();
        configProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        configProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.ByteArraySerializer");
        configProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");

        org.apache.kafka.clients.producer.Producer producer = new KafkaProducer(configProperties);
        //String line = in.nextLine();
//        while(!line.equals("exit")) {
//            //TODO: Make sure to use the ProducerRecord constructor that does not take parition Id
//            ProducerRecord<String, String> rec = new ProducerRecord<String, String>(topicName,line);
//            producer.send(rec);
//            line = in.nextLine();
//        }

        for(int i =0; i < messageArray.length; i++)
        {
            ProducerRecord<String, String> rec = new ProducerRecord<String, String>(topicName,(String)(messageArray[i]));
            producer.send(rec);
            System.out.println("Producer: " + rec.value());
            Thread.sleep(5000);


        }
        //in.close();
        producer.close();
    }
}
