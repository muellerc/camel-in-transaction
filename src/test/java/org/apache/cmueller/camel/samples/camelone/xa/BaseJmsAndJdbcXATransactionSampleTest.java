package org.apache.cmueller.camel.samples.camelone.xa;

import java.sql.SQLException;
import java.util.concurrent.CountDownLatch;

import org.apache.activemq.broker.BrokerService;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.test.spring.CamelSpringTestSupport;
import org.apache.cmueller.camel.samples.camelone.ActiveMQUtil;
import org.apache.cmueller.camel.samples.camelone.DatabaseUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.support.TransactionTemplate;

public abstract class BaseJmsAndJdbcXATransactionSampleTest extends CamelSpringTestSupport {

    private BrokerService broker;
    private JdbcTemplate jdbc;
    private TransactionTemplate transactionTemplate;

    private CountDownLatch latch;

    @Before
    @Override
    public void setUp() throws Exception {
        broker = ActiveMQUtil.createAndStartBroker();

        super.setUp();

        jdbc = DatabaseUtil.createJdbcTemplate(context);
        transactionTemplate = DatabaseUtil.createTransactionTemplate(context, "jtaTransactionManager");
        DatabaseUtil.createAndInitializeDatabase(transactionTemplate, jdbc);
    }

    @After
    @Override
    public void tearDown() throws Exception {
        DatabaseUtil.dropDatabase(transactionTemplate, jdbc);

        super.tearDown();

        ActiveMQUtil.stopBroker(broker);
    }

    @Test
    public void moneyShouldBeTransfered() {
        assertEquals(1000, DatabaseUtil.queryForLong(transactionTemplate, jdbc, "SELECT balance from account where name = 'foo'"));
        assertEquals(1000, DatabaseUtil.queryForLong(transactionTemplate, jdbc, "SELECT balance from account where name = 'bar'"));

        template.sendBody("activemq:queue:transaction.incoming.one", new Long(100));

        Exchange exchange = consumer.receive("activemq:queue:transaction.outgoing.one", 5000);
        assertNotNull(exchange);

        assertEquals(900, DatabaseUtil.queryForLong(transactionTemplate, jdbc, "SELECT balance from account where name = 'foo'"));
        assertEquals(1100, DatabaseUtil.queryForLong(transactionTemplate, jdbc, "SELECT balance from account where name = 'bar'"));
    }

    @Test
    public void moneyShouldNotTransfered() {
        assertEquals(1000, DatabaseUtil.queryForLong(transactionTemplate, jdbc, "SELECT balance from account where name = 'foo'"));
        assertEquals(1000, DatabaseUtil.queryForLong(transactionTemplate, jdbc, "SELECT balance from account where name = 'bar'"));

        template.sendBody("activemq:queue:transaction.incoming.two", new Long(100));

        Exchange exchange = consumer.receive("activemq:queue:ActiveMQ.DLQ", 5000);
        assertNotNull(exchange);

        assertEquals(1000, DatabaseUtil.queryForLong(transactionTemplate, jdbc, "SELECT balance from account where name = 'foo'"));
        assertEquals(1000, DatabaseUtil.queryForLong(transactionTemplate, jdbc, "SELECT balance from account where name = 'bar'"));
    }

    @Test
    public void moneyShouldNotTransfered2() throws Exception {
        assertEquals(1000, DatabaseUtil.queryForLong(transactionTemplate, jdbc, "SELECT balance from account where name = 'foo'"));
        assertEquals(1000, DatabaseUtil.queryForLong(transactionTemplate, jdbc, "SELECT balance from account where name = 'bar'"));

        template.sendBody("activemq:queue:transaction.incoming.three", new Long(100));

        Exchange exchange = consumer.receive("activemq:queue:ActiveMQ.DLQ", 5000);
        assertNotNull(exchange);

        assertEquals(1000, DatabaseUtil.queryForLong(transactionTemplate, jdbc, "SELECT balance from account where name = 'foo'"));
        assertEquals(1000, DatabaseUtil.queryForLong(transactionTemplate, jdbc, "SELECT balance from account where name = 'bar'"));
    }

    @Test
    public void perfTest() throws Exception {
        assertEquals(1000, DatabaseUtil.queryForLong(transactionTemplate, jdbc, "SELECT balance from account where name = 'foo'"));
        assertEquals(1000, DatabaseUtil.queryForLong(transactionTemplate, jdbc, "SELECT balance from account where name = 'bar'"));

        // warm up
        latch = new CountDownLatch(100);
        for (int i = 0; i < 100; i++) {
            template.sendBody("activemq:queue:transaction.incoming.four", new Long(0));
        }
        latch.await();

        latch = new CountDownLatch(1000);
        long start = System.currentTimeMillis();
        for (int i = 0; i < 1000; i++) {
            template.sendBody("activemq:queue:transaction.incoming.four", new Long(1));
        }
        latch.await();
        long end = System.currentTimeMillis();

        System.out.println("duration: " + (end - start) + "ms");

        assertEquals(0, DatabaseUtil.queryForLong(transactionTemplate, jdbc, "SELECT balance from account where name = 'foo'"));
        assertEquals(2000, DatabaseUtil.queryForLong(transactionTemplate, jdbc, "SELECT balance from account where name = 'bar'"));
    }

    @Override
    protected RouteBuilder createRouteBuilder() throws Exception {
        return new RouteBuilder() {
            public void configure() throws Exception {
                from("activemqXa:queue:transaction.incoming.one")
                    .transacted("PROPAGATION_REQUIRED")
                    .to("sql:UPDATE account SET balance = (SELECT balance from account where name = 'foo') - # WHERE name = 'foo'?dataSource=dataSource")
                    .to("sql:UPDATE account SET balance = (SELECT balance from account where name = 'bar') + # WHERE name = 'bar'?dataSource=dataSource")
                    .to("activemqXa:queue:transaction.outgoing.one");

                from("activemqXa:queue:transaction.incoming.two")
                    .transacted("PROPAGATION_REQUIRED")
                    .to("sql:UPDATE account SET balance = (SELECT balance from account where name = 'foo') - # WHERE name = 'foo'?dataSource=dataSource")
                    .throwException(new SQLException("forced exception for test"))
                    .to("sql:UPDATE account SET balance = (SELECT balance from account where name = 'bar') + # WHERE name = 'bar'?dataSource=dataSource")
                    .to("activemqXa:queue:transaction.outgoing.two");

                from("activemqXa:queue:transaction.incoming.three")
                    .transacted("PROPAGATION_REQUIRED")
                    .to("sql:UPDATE account SET balance = (SELECT balance from account where name = 'foo') - # WHERE name = 'foo'?dataSource=dataSource")
                    .to("sql:UPDATE account SET balance = (SELECT balance from account where name = 'bar') + # WHERE name = 'bar'?dataSource=dataSource")
                    .throwException(new SQLException("forced exception for test"))
                    .to("activemqXa:queue:transaction.outgoing.three");

                from("activemqXa:queue:transaction.incoming.four")
                    .transacted("PROPAGATION_REQUIRED")
                    .to("sql:UPDATE account SET balance = (SELECT balance from account where name = 'foo') - # WHERE name = 'foo'?dataSource=dataSource")
                    .to("sql:UPDATE account SET balance = (SELECT balance from account where name = 'bar') + # WHERE name = 'bar'?dataSource=dataSource")
                    .to("activemqXa:queue:transaction.outgoing.four")
                    .process(new Processor() {
                        public void process(Exchange exchange) throws Exception {
                            latch.countDown();
                        }
                    });
            }
        };
    }
}