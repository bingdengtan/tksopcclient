package com.je.tks;

import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Observable;
import java.util.Observer;

public class OPCReadData {
    static final Logger logger = Logger.getLogger(OPCReadData.class);

    public static void main(String[] args) throws Exception {
        /*
        the args should be:
        1: domain, 2: host, 3: user name, 4: password, 5 progid
         */
        logger.info("=================Start OPC Client=================");
        if(args.length < 6){
            logger.error("Invalid number of parameter");
            return;
        }
        logger.info("Domain: " + args[0] + ", Host: " + args[1] + ", Username: " + args[2] + ", ProgID: " + args[4] + ", Item ID: " + args[5]);
        //Matrikon.OPC.Simulation.1  Takebishi.Dxp.5

        OpcClient opcClient = new OpcClient();
        opcClient.showAllOPCServer(args[1], args[2], args[3], args[0]);

        boolean ret = opcClient.connectServer(args[1], args[4], args[2], args[3], args[0]);
        if (!ret) {
            logger.error("Connect opc server fail");
            return;
        }
        logger.info("Connect opc server success!");

        List<String> itemIdList = new ArrayList<String>();

        itemIdList.add(args[5]);
        ret = opcClient.checkItemList(itemIdList);
        if (!ret) {
            System.out.println("not contain item list");
            logger.error("not contain item list");
            return;
        }

        opcClient.subscribe(new Observer() {
            public void update(Observable observable, Object arg) {
                Result result = (Result) arg;
                logger.debug("update result=" + result);
                System.out.println("update result=" + result);
            }
        });
        opcClient.syncReadObject("DM.A", 1000);
        delay(5 * 60 * 1000);
    }

    private static void delay(long time) {
        try {
            Thread.sleep(time);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
