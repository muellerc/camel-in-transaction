package org.apache.cmueller.camel.samples.camelone;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.usage.SystemUsage;
import org.apache.activemq.usage.TempUsage;

public class ActiveMQUtil {

    public static BrokerService createAndStartBroker() throws Exception {
        BrokerService broker = new BrokerService();
        broker.setPersistent(false);
        broker.setUseJmx(false);
        broker.setBrokerName("localhost");
        broker.addConnector("tcp://localhost:61616");
        SystemUsage systemUsage = new SystemUsage();
        TempUsage tempUsage = new TempUsage();
        tempUsage.setLimit(52428800L);
        systemUsage.setTempUsage(tempUsage);
        broker.setSystemUsage(systemUsage);
        broker.start();

        return broker;
    }
    
    public static void stopBroker(BrokerService broker) throws Exception {
        if (broker != null) {
            broker.stop();
        }
    }
}