package org.apache.cmueller.camel.samples.camelone.tx;

import java.sql.SQLException;

import org.apache.activemq.broker.BrokerService;
import org.apache.camel.Exchange;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.test.spring.CamelSpringTestSupport;
import org.apache.cmueller.camel.samples.camelone.ActiveMQUtil;
import org.apache.cmueller.camel.samples.camelone.DatabaseUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.support.TransactionTemplate;

public class JmsAndJdbcCompensationTransactionSampleTest extends CamelSpringTestSupport {

    private BrokerService broker;
    private JdbcTemplate jdbc;
    private TransactionTemplate transactionTemplate;

    @Before
    @Override
    public void setUp() throws Exception {
        broker = ActiveMQUtil.createAndStartBroker();

        super.setUp();

        jdbc = DatabaseUtil.createJdbcTemplate(context);
        transactionTemplate = DatabaseUtil.createTransactionTemplate(context, "dataSourceTransactionManager");
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
    public void moneyShouldTransfer() {
        assertEquals(1000, DatabaseUtil.queryForLong(transactionTemplate, jdbc, "SELECT balance from account where name = 'foo'"));
        assertEquals(1000, DatabaseUtil.queryForLong(transactionTemplate, jdbc, "SELECT balance from account where name = 'bar'"));

        template.sendBody("activemq:queue:transaction.incoming.one", 100L);

        Exchange exchange = consumer.receive("activemq:queue:transaction.outgoing.one", 5000);
        assertNotNull(exchange);

        assertEquals(900, DatabaseUtil.queryForLong(transactionTemplate, jdbc, "SELECT balance from account where name = 'foo'"));
        assertEquals(1100, DatabaseUtil.queryForLong(transactionTemplate, jdbc, "SELECT balance from account where name = 'bar'"));
    }

    @Test
    public void moneyShouldNotTransferWhenExceptionInBetweenUpdates() {
        assertEquals(1000, DatabaseUtil.queryForLong(transactionTemplate, jdbc, "SELECT balance from account where name = 'foo'"));
        assertEquals(1000, DatabaseUtil.queryForLong(transactionTemplate, jdbc, "SELECT balance from account where name = 'bar'"));

        template.sendBody("activemq:queue:transaction.incoming.two", 100L);

        Exchange exchange = consumer.receive("activemq:queue:ActiveMQ.DLQ", 5000);
        assertNotNull(exchange);

        assertEquals(1000, DatabaseUtil.queryForLong(transactionTemplate, jdbc, "SELECT balance from account where name = 'foo'"));
        assertEquals(1000, DatabaseUtil.queryForLong(transactionTemplate, jdbc, "SELECT balance from account where name = 'bar'"));
    }

    @Test
    public void moneyShouldNotTransferWhenExceptionAfterUpdates() {
        assertEquals(1000, DatabaseUtil.queryForLong(transactionTemplate, jdbc, "SELECT balance from account where name = 'foo'"));
        assertEquals(1000, DatabaseUtil.queryForLong(transactionTemplate, jdbc, "SELECT balance from account where name = 'bar'"));

        template.sendBody("activemq:queue:transaction.incoming.three", 100L);

        Exchange exchange = consumer.receive("activemq:queue:ActiveMQ.DLQ", 5000);
        assertNotNull(exchange);

        assertEquals(1000, DatabaseUtil.queryForLong(transactionTemplate, jdbc, "SELECT balance from account where name = 'foo'"));
        assertEquals(1000, DatabaseUtil.queryForLong(transactionTemplate, jdbc, "SELECT balance from account where name = 'bar'"));
    }

    @Override
    protected RouteBuilder createRouteBuilder() throws Exception {
        return new RouteBuilder() {
            @Override
            public void configure() throws Exception {
                context.setTracing(true);

                from("activemqTx:queue:transaction.incoming.one")
                    .transacted("PROPAGATION_REQUIRED_JDBC")
                    .to("sql:UPDATE account SET balance = (SELECT balance from account where name = 'foo') - # WHERE name = 'foo'?dataSource=dataSource")
                    .to("sql:UPDATE account SET balance = (SELECT balance from account where name = 'bar') + # WHERE name = 'bar'?dataSource=dataSource")
                    .to("activemqTx:queue:transaction.outgoing.one");

                from("activemqTx:queue:transaction.incoming.two")
                    .transacted("PROPAGATION_REQUIRED_JDBC")
                    .to("sql:UPDATE account SET balance = (SELECT balance from account where name = 'foo') - # WHERE name = 'foo'?dataSource=dataSource")
                    .throwException(new SQLException("forced exception for test"))
                    .to("sql:UPDATE account SET balance = (SELECT balance from account where name = 'bar') + # WHERE name = 'bar'?dataSource=dataSource")
                    .to("activemqTx:queue:transaction.outgoing.two");

                from("activemqTx:queue:transaction.incoming.three")
                    .transacted("PROPAGATION_REQUIRED_JDBC")
                    .to("sql:UPDATE account SET balance = (SELECT balance from account where name = 'foo') - # WHERE name = 'foo'?dataSource=dataSource")
                    .to("sql:UPDATE account SET balance = (SELECT balance from account where name = 'bar') + # WHERE name = 'bar'?dataSource=dataSource")
                    .throwException(new SQLException("forced exception for test"))
                    .to("activemqTx:queue:transaction.outgoing.three");
            }
        };
    }

    @Override
    protected AbstractApplicationContext createApplicationContext() {
        return new ClassPathXmlApplicationContext(
            "META-INF/spring/JmsAndJdbcCompensationTransactionSampleTest-context.xml");
    }
}