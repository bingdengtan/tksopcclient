package com.je.tks;

import java.util.ArrayList;
import java.util.List;

import org.openscada.opc.lib.da.Group;
import org.openscada.opc.lib.da.Item;
import org.openscada.opc.lib.da.Server;

public class PLC {
    public Server server;
    public String name;
    public String[] tags;
    public List<Item> items;

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

    public String getValues() throws Exception {
        String msg = "";

        for(int i = 0; i < this.getItems().size(); i++){
            Item item = this.getItems().get(i);
            String val = "<" + (i+3) + ">" + item.read(false).getValue().getObject().toString();
            msg += msg == "" ? val : "|" + val;
        }
        return msg;
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
