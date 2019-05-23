package com.je.tks;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
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
            String pbc = "";
            Random rand = new Random();
            DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

            while (true) {
                if(this.server.active) {
                    List<PLC> plcs = this.server.getPlcs();
                    for(int j=0;j<plcs.size();j++){
                        PLC plc = plcs.get(j);
                        int random = rand.nextInt(10);
                        Date date = new Date();
                        pbc = random > 1 ? "PBC" : "";
                        message = "OPC@MD046Z_010|1|A|" + dateFormat.format(date) + "|C30aTxG3OL|||3T1234W7SBD007GD|" + pbc + "|1999-5YY0746|<1>PASS|<2>OPC";
                        try {
                            message += "|" + plc.getValues();
                            ProducerRecord<String, String> msg = new ProducerRecord<String, String>(this.server.topic, message);
                            this.producer.send(msg);

                            logger.info("OPC server [" + this.server.description + "]" + " send message completed: " + "Topic: " + this.server.topic + ", " + message);
                            System.out.println("OPC server [" + this.server.description + "]" + " send message completed: " + this.server.topic + ", " + message);

                            sleep(1000);
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
                    //this.server.initServer();
                }
            }
        }catch (Exception e) {
            e.printStackTrace();
            logger.error("Thread run failed: " + e.toString());
        }
    }
}
