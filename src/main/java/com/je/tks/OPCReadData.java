package com.je.tks;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Properties;
import java.io.File;
import java.io.FileReader;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

public class OPCReadData {
    static final Logger logger = LoggerFactory.getLogger(OPCReadData.class);

    public static void main(String[] args) throws Exception {
        // get the configuration file path.
        String configFilePath = new File(OPCReadData.class.getProtectionDomain().getCodeSource().getLocation()
                .toURI()).getPath();
        File jar = new File(configFilePath);
        configFilePath = jar.getParent() + File.separator + "config.json";

        if(args != null && args.length > 0) {
            logger.error("Configure file path from args: " + args[0]);
            configFilePath = args[0];
        }else{
            logger.error("Configure file path: " + configFilePath);
        }

        if(!new File(configFilePath).exists()){
            logger.error("File is not exists: " + configFilePath);
            return;
        }

        // read the configuration file.
        JSONParser parser = new JSONParser();
        JSONObject jsonObject = (JSONObject) parser.parse(new FileReader(configFilePath));

        // init kafka
        JSONObject jsonKafka = (JSONObject) jsonObject.get("kafka");
        final Properties props = new Properties();
        props.put("bootstrap.servers", jsonKafka.get("bootstrapServers").toString());
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        Producer<String, String> producer = new KafkaProducer<String,String>(props);

        //opc servers
        JSONArray jsonOPCServers = (JSONArray) jsonObject.get("opc_servers");
        for(int i=0; i<jsonOPCServers.size();i++){
            JSONObject jsonServer = (JSONObject) jsonOPCServers.get(i);
            OPCServer opcServer = new OPCServer(jsonServer);
            if(opcServer.checkItemList()) {
                SendMessage2Kafka send = new SendMessage2Kafka(producer, opcServer);
                send.start();
            }
        }
    }
}