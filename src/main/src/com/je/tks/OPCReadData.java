package com.je.tks;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.jinterop.dcom.common.JIException;
import org.jinterop.dcom.core.JIVariant;
import org.openscada.opc.lib.common.ConnectionInformation;
import org.openscada.opc.lib.da.AccessBase;
import org.openscada.opc.lib.da.DataCallback;
import org.openscada.opc.lib.da.Group;
import org.openscada.opc.lib.da.Item;
import org.openscada.opc.lib.da.ItemState;
import org.openscada.opc.lib.da.Server;
import org.openscada.opc.lib.da.SyncAccess;

public class OPCReadData {
    public static void main(String[] args) throws Exception {
        //https://www.cnblogs.com/ioufev/p/9928971.html
        final ConnectionInformation ci = new ConnectionInformation();

        ci.setHost("DESKTOP-QU0VA2N");          // 电脑IP
        ci.setDomain("");                   // 域，为空就行
        //ci.setUser("dell");              // 用户名，配置DCOM时配置的
        //ci.setPassword("440781Tbdd");           // 密码

        ci.setClsid("F8582CF3-88FB-11D0-B850-00C0F0104305");
        final Server server = new Server(ci, Executors.newSingleThreadScheduledExecutor());
        try{
            server.connect();

            final AccessBase access = new SyncAccess(server, 500);
        }catch (final JIException e) {
            System.out.println(String.format("%08X: %s", e.getErrorCode(), server.getErrorMessage(e.getErrorCode())));
        }

    }
}
