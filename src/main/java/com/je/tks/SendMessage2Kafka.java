package com.je.tks;

import com.sun.deploy.util.StringUtils;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.log4j.Logger;
import org.jinterop.dcom.common.JIException;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Random;

public class SendMessage2Kafka extends Thread {
    static final Logger logger = Logger.getLogger(OPCReadData.class);

    private Producer<String, String> producer;
    private OPCServer server;
    private String name;

    public SendMessage2Kafka(Producer producer, OPCServer server){
        this.producer = producer;
        this.server = server;
        this.name = server.description;
    }

    @Override
    public void run(){
        try {
            String message = "";
            logger.info("Thread for server " + this.server.description + " is running...");

            while (true) {
                if(this.server.active) {
                    List<PLC> plcs = this.server.getPlcs();
                    for(int j=0;j<plcs.size();j++){
                        PLC plc = plcs.get(j);
                        String messageFormater = plc.kafkaMessage;
                        try {
                            String[] values = plc.getValuesArray();
                            String messageUniqueKey = this.getMessageUniqueKey(plc, values);
                            if( !messageUniqueKey.equals(plc.getPrevalue())){
                                System.out.println("PRE Key: " + plc.getPrevalue() + ", CUR Key: " + messageUniqueKey);
                                plc.setPrevalue(messageUniqueKey);
                                message = String.format(messageFormater, values);
                                ProducerRecord<String, String> msg = new ProducerRecord<String, String>(plc.kafkaTopic, values[plc.getKafkaKey4Tag()], message);
                                this.producer.send(msg, new Callback() {
                                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                                        String sendResponse = "Message sent to topic ->"  + recordMetadata.topic()+ " ,parition->" + recordMetadata.partition() +" stored at offset->" + recordMetadata.offset();
                                        System.out.println(sendResponse);
                                        // logger.info("Kafka send response: " + sendResponse);
                                    }
                                });

                                logger.info("OPC server [" + this.server.description + "], PLC [" + plc.name + "]" + " send message completed: " + "Topic: " + plc.kafkaTopic + ", " + message);
                                System.out.println("OPC server [" + this.server.description + "], PLC [" + plc.name + "]" + " send message completed: " + "Topic: " + plc.kafkaTopic + ", " + message);

                                sleep(plc.interval);
                            }else {
                                sleep(plc.interval);
                            }
                        }catch (JIException e){
                            this.server.active = false;
                            logger.error("Get data from OPC server " + this.server.description + " failed: " + e.toString());
                            System.out.println("Get data from OPC server " + this.server.description + " failed: " + e.toString());
                            stop();
                        }catch (Exception e){
                            logger.error("Send message to kafka failed: " + e.toString());
                            System.out.println("Send message to kafka failed: " + e.toString());
                            stop();
                        }
                    }
                }else{
                    logger.error("Connect to OPC server failed.");
                }
            }
        }catch (Exception e) {
            e.printStackTrace();
            logger.error("Thread run failed: " + e.toString());
        }
    }

    private String getMessageUniqueKey(PLC plc, String[] values){
        String key = "";

        if(plc.getMessageUnique4Tags().length > 0){
            for(int i=0;i<plc.getMessageUnique4Tags().length;i++){
                key += key.equals("") ? values[plc.getMessageUnique4Tags()[i]-1] : "|" + values[plc.getMessageUnique4Tags()[i] - 1];
            }
        }else{
            for(int i=0;i<values.length;i++){
                key += key.equals("") ? values[i] : "|" + values[i];
            }
        }
        return key;
    }
}
