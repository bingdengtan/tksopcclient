package com.je.tks;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.List;

import org.openscada.opc.lib.da.Group;
import org.openscada.opc.lib.da.Item;
import org.openscada.opc.lib.da.Server;

public class PLC {
    public Server server;
    public String name;
    public String prevalue;
    public String[] tags;
    public String kafkaTopic;
    public String kafkaMessage;
    public int[] messageUnique4Tags;
    public int kafkaKey4Tag;
    public int interval;
    public List<Item> items;

    public int[] getMessageUnique4Tags() {
        return messageUnique4Tags;
    }

    public void setMessageUnique4Tags(int[] messageUnique4Tags) {
        this.messageUnique4Tags = messageUnique4Tags;
    }

    public int getKafkaKey4Tag() {
        return kafkaKey4Tag;
    }

    public void setKafkaKey4Tag(int kafkaKey4Tag) {
        this.kafkaKey4Tag = kafkaKey4Tag;
    }

    public String getKafkaTopic() {
        return kafkaTopic;
    }

    public void setKafkaTopic(String kafkaTopic) {
        this.kafkaTopic = kafkaTopic;
    }

    public String getKafkaMessage() {
        return kafkaMessage;
    }

    public void setKafkaMessage(String kafkaMessage) {
        this.kafkaMessage = kafkaMessage;
    }


    public int getInterval() {
        return interval;
    }

    public void setInterval(int interval) {
        this.interval = interval;
    }

    public List<Item> getItems() throws Exception{
        if(this.items == null){
            List<Item> itemIdList = new ArrayList<Item>();
            Group group = this.server.addGroup();
            for(int i=0;i<this.tags.length;i++){
                Item item = group.addItem(this.tags[i]);
                itemIdList.add(item);
            }
            this.items = itemIdList;
        }
        return this.items;
    }

    public String getValuesString() throws Exception {
        String msg = "";

        for(int i = 0; i < this.getItems().size(); i++){
            Item item = this.getItems().get(i);
            String val = item.read(false).getValue().getObjectAsString().getString();
            msg += msg == "" ? val : "|" + val;
        }
        return msg;
    }

    public String[] getValuesArray() throws Exception {
        String msg = "";

        msg = this.getValuesString();
        return msg.split("\\|");
    }

    public String getPrevalue() {
        return prevalue;
    }

    public void setPrevalue(String prevalue) {
        this.prevalue = prevalue;
    }

    public Server getServer() {
        return server;
    }

    public void setServer(Server server) {
        this.server = server;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String[] getTags() {
        return tags;
    }

    public void setTags(String[] tags) {
        this.tags = tags;
    }


}
