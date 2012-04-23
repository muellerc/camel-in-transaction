package org.apache.cmueller.camel.samples.camelone.jms;

import java.sql.SQLException;

import org.apache.camel.Exchange;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.test.junit4.CamelSpringTestSupport;
import org.junit.Test;
import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

public class JmsTransactionSample extends CamelSpringTestSupport {
	
	@Test
	public void moneyShouldBeTransfered() {
		template.sendBodyAndHeader("activemq:queue:transaction.incoming.one", "Camel rocks!", "amount", "100");
		
		Exchange exchange = consumer.receive("activemq:queue:transaction.outgoing.one", 5000);
		assertNotNull(exchange);
	}
	
	@Test
	public void moneyShouldNotTransfered() {
		template.sendBodyAndHeader("activemq:queue:transaction.incoming.two", "Camel rocks!", "amount", "100");
		
		Exchange exchange = consumer.receive("activemq:queue:ActiveMQ.DLQ", 5000);
		assertNotNull(exchange);
	}
	
    @Override
    protected RouteBuilder createRouteBuilder() throws Exception {
        return new RouteBuilder() {
            @Override
            public void configure() throws Exception {
                from("activemqTx:queue:transaction.incoming.one")
                    .transacted("PROPAGATION_REQUIRED")
                    .to("activemqTx:queue:transaction.outgoing.one");
                
                from("activemqTx:queue:transaction.incoming.two")
	                .transacted("PROPAGATION_REQUIRED")
	                .throwException(new SQLException("forced exception for test"))
	                .to("activemqTx:queue:transaction.outgoing.two");
            }
        };
    }
    
	@Override
	protected AbstractApplicationContext createApplicationContext() {
		return new ClassPathXmlApplicationContext("META-INF/spring/JmsTransactionSample-context.xml");
	}
}