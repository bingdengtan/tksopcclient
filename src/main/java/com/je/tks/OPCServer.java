package com.je.tks;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.google.gson.JsonArray;
import org.apache.log4j.Logger;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.openscada.opc.lib.da.Group;
import org.openscada.opc.lib.da.Item;
import org.openscada.opc.lib.da.Server;
import com.google.gson.Gson;

public class OPCServer {
    static final Logger logger = Logger.getLogger(OPCReadData.class);

    public Server server;
    public String description;
    public String domain;
    public String host;
    public String username;
    public String password;
    public String clsid;
    public String topic;
    public List<PLC> plcs;
    public boolean active;

    private OpcClient opcClient;

    public Server getServer() {
        return server;
    }

    public void setServer(Server server) {
        this.server = server;
    }

    public OpcClient getOpcClient() {
        return opcClient;
    }

    public void setOpcClient(OpcClient opcClient) {
        this.opcClient = opcClient;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getClsid() {
        return clsid;
    }

    public void setClsid(String clsid) {
        this.clsid = clsid;
    }

    public List getPlcs() {
        return plcs;
    }

    public void setPlcs(JSONArray plcs) {
        List<PLC> lsPLC = new ArrayList<PLC>();
        for(int i=0;i<plcs.size();i++){
            JSONObject obj = (JSONObject) plcs.get(i);
            Gson gson = new Gson();
            PLC plc = gson.fromJson(obj.toJSONString(), PLC.class);
            plc.server = this.server;
            lsPLC.add(plc);
        }
        this.plcs = lsPLC;
    }

    public OPCServer(String description, String domain, String host, String username, String password, String clsId, JSONArray plcs, String kafkaTopic){
        this.description = description;
        this.domain = domain;
        this.host = host;
        this.username = username;
        this.password = password;
        this.clsid = clsId;
        this.topic = kafkaTopic;
        this.initServer();

        this.setPlcs(plcs);
    }

    public OPCServer(JSONObject jsonServer){
        this.description = jsonServer.get("description").toString();
        this.host = jsonServer.get("host").toString();
        this.domain = jsonServer.get("domain").toString();
        this.username = jsonServer.get("username").toString();
        this.password = jsonServer.get("password").toString();
        this.clsid = jsonServer.get("clsid").toString();
        this.topic = jsonServer.get("kafka_topic").toString();
        this.initServer();

        JSONArray plcs = (JSONArray) jsonServer.get("plcs");
        this.setPlcs(plcs);
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getDomain() {
        return domain;
    }

    public void setDomain(String domain) {
        this.domain = domain;
    }

    public void initServer(){
        OpcClient opcClient = new OpcClient();
        boolean ret = opcClient.connectServer(this.host, this.clsid, this.username,this.password, this.domain);
        if (!ret) {
            logger.error("Connect opc server fail");
            this.active = false;
            return;
        }
        this.active = true;
        this.server = opcClient.mServer;
        this.opcClient = opcClient;
        logger.info("Connect opc server success!");
    }

    public boolean checkItemList() {
        boolean result = true;

        List<PLC> plcs = this.getPlcs();
        for(int i=0;i<plcs.size();i++) {
            String[] tags = plcs.get(i).tags;
            List<String> list = Arrays.asList(tags);
            if(!this.opcClient.checkItemList(list)) {
                logger.error("Items not found in OPC server: " + list);
                System.out.println("Items not found in OPC server: " + list);
                result = false;
                break;
            }
        }

        return result;
    }
}
