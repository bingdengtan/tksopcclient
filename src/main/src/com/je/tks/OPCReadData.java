package com.je.tks;

import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Observable;
import java.util.Observer;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;

public class OPCReadData {
    static final Logger logger = Logger.getLogger(OPCReadData.class);

    public static void main(String[] args) throws Exception {
        final Properties props = new Properties();
        props.put("bootstrap.servers", "edge02.jehl.internal:9092,edge03.jehl.internal:9092,edge04.jehl.internal:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        /*
        the args should be:
        1: domain, 2: host, 3: user name, 4: password, 5 progid
         */
        logger.info("=================Start OPC Client=================");
        if(args.length < 7){
            logger.error("Invalid number of parameter");
            return;
        }
        final String topic = args[6];

        System.out.println("Domain: " + args[0] + ", Host: " + args[1] + ", Username: " + args[2] + ", ProgID: " + args[4] + ", Item ID: " + args[5] + ", topic: " + topic);
        logger.info("Domain: " + args[0] + ", Host: " + args[1] + ", Username: " + args[2] + ", ProgID/clsid: " + args[4] + ", Item ID: " + args[5] + ", topic: " + topic);
        //Matrikon.OPC.Simulation.1  Takebishi.Dxp.5

        OpcClient opcClient = new OpcClient();
        //opcClient.showAllOPCServer(args[1], args[2], args[3], args[0]);

        boolean ret = opcClient.connectServer(args[1], args[4], args[2], args[3], args[0]);
        if (!ret) {
            logger.error("Connect opc server fail");
            return;
        }
        logger.info("Connect opc server success!");

        List<String> itemIdList = new ArrayList<String>();

        String[] tags = args[5].split(",");
        for(int i = 0; i<tags.length; i++){
            System.out.println(tags[i]);
            itemIdList.add(tags[i]);
        }

        ret = opcClient.checkItemList(itemIdList);
        if (!ret) {
            System.out.println("not contain item list");
            logger.error("not contain item list");
            return;
        }

        final Producer<String, String> procuder = new KafkaProducer<String,String>(props);
        opcClient.subscribe(new Observer() {
            public void update(Observable observable, Object arg) {
                Result result = (Result) arg;
                logger.debug("update result=" + result);
                System.out.println("update result=" + result);
                ProducerRecord<String, String> msg = new ProducerRecord<String, String>(topic, result.getValue().toString());
                procuder.send(msg);
                logger.debug("Send message to kafka completed!");
            }
        });

        String message = "";
        String pbc = "";
        Random rand = new Random();
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        while (true){
            int random = rand.nextInt(10);
            Date date = new Date();
            pbc = random > 1 ? "PBC" : "";
            message = "OPC@MD046Z_010|1|A|" + dateFormat.format(date) + "|C30aTxG3OL|||3T1234W7SBD007GD|" + pbc + "|1999-5YY0746|<1>PASS|<2>OPC";
            for(int i = 0; i < tags.length; i++){
                String val = opcClient.readData(tags[i]);
                message += "|<" + (i+3) + ">" + val;
            }
            logger.debug("Kafka message: " + message);
            System.out.println("Kafka message: " + message);
            ProducerRecord<String, String> msg = new ProducerRecord<String, String>(topic, message);
            logger.debug("Send message to kafka completed!");

            procuder.send(msg);

            Thread.sleep(1000);
        }
        //opcClient.asyncReadObject(args[5], 1000);
        //delay(24 * 60 * 60 * 1000);
        //opcClient.disconnectServer();
    }

    private static void delay(long time) {
        try {
            Thread.sleep(time);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
