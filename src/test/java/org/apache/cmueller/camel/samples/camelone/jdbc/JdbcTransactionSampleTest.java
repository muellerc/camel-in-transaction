package org.apache.cmueller.camel.samples.camelone.jdbc;

import java.sql.SQLException;

import org.apache.camel.Exchange;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.test.spring.CamelSpringTestSupport;
import org.apache.cmueller.camel.samples.camelone.DatabaseUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.support.TransactionTemplate;

public class JdbcTransactionSampleTest extends CamelSpringTestSupport {

    private JdbcTemplate jdbc;
    private TransactionTemplate transactionTemplate;

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();

        jdbc = DatabaseUtil.createJdbcTemplate(context);
        transactionTemplate = DatabaseUtil.createTransactionTemplate(context, "transactionManager");
        DatabaseUtil.createAndInitializeDatabase(transactionTemplate, jdbc);
    }

    @After
    @Override
    public void tearDown() throws Exception {
        DatabaseUtil.dropDatabase(transactionTemplate, jdbc);

        super.tearDown();
    }

    @Test
    public void moneyShouldBeTransfered() throws Exception {
        assertEquals(1000, DatabaseUtil.queryForLong(transactionTemplate, jdbc, "SELECT balance from account where name = 'foo'"));
        assertEquals(1000, DatabaseUtil.queryForLong(transactionTemplate, jdbc, "SELECT balance from account where name = 'bar'"));

        template.sendBody("seda:transaction.incoming.one", new Long(100));

        Exchange exchange = consumer.receive("seda:transaction.outgoing.one", 5000);
        assertNotNull(exchange);

        assertEquals(900, DatabaseUtil.queryForLong(transactionTemplate, jdbc, "SELECT balance from account where name = 'foo'"));
        assertEquals(1100, DatabaseUtil.queryForLong(transactionTemplate, jdbc, "SELECT balance from account where name = 'bar'"));
    }

    @Test
    public void moneyShouldNotTransfered() throws Exception {
        assertEquals(1000, DatabaseUtil.queryForLong(transactionTemplate, jdbc, "SELECT balance from account where name = 'foo'"));
        assertEquals(1000, DatabaseUtil.queryForLong(transactionTemplate, jdbc, "SELECT balance from account where name = 'bar'"));

        template.sendBody("seda:transaction.incoming.two", new Long(100));
        Thread.sleep(2000);

        assertEquals(1000, DatabaseUtil.queryForLong(transactionTemplate, jdbc, "SELECT balance from account where name = 'foo'"));
        assertEquals(1000, DatabaseUtil.queryForLong(transactionTemplate, jdbc, "SELECT balance from account where name = 'bar'"));
    }

    @Test
    public void moneyShouldNotTransfered2() throws Exception {
        assertEquals(1000, DatabaseUtil.queryForLong(transactionTemplate, jdbc, "SELECT balance from account where name = 'foo'"));
        assertEquals(1000, DatabaseUtil.queryForLong(transactionTemplate, jdbc, "SELECT balance from account where name = 'bar'"));

        template.sendBody("seda:transaction.incoming.three", new Long(100));
        Thread.sleep(2000);

        assertEquals(1000, DatabaseUtil.queryForLong(transactionTemplate, jdbc, "SELECT balance from account where name = 'foo'"));
        assertEquals(1000, DatabaseUtil.queryForLong(transactionTemplate, jdbc, "SELECT balance from account where name = 'bar'"));
    }

    @Override
    protected RouteBuilder createRouteBuilder() throws Exception {
        return new RouteBuilder() {
            @Override
            public void configure() throws Exception {
                from("seda:transaction.incoming.one")
                    .transacted("PROPAGATION_REQUIRED")
                    .to("sql:UPDATE account SET balance = (SELECT balance from account where name = 'foo') - # WHERE name = 'foo'?dataSource=dataSource")
                    .to("sql:UPDATE account SET balance = (SELECT balance from account where name = 'bar') + # WHERE name = 'bar'?dataSource=dataSource")
                    .to("seda:transaction.outgoing.one");

                from("seda:transaction.incoming.two")
                    .transacted("PROPAGATION_REQUIRED")
                    .to("sql:UPDATE account SET balance = (SELECT balance from account where name = 'foo') - # WHERE name = 'foo'?dataSource=dataSource")
                    .throwException(new SQLException("forced exception for test"))
                    .to("sql:UPDATE account SET balance = (SELECT balance from account where name = 'bar') + # WHERE name = 'bar'?dataSource=dataSource")
                    .to("seda:transaction.outgoing.two");

                from("seda:transaction.incoming.three")
                    .transacted("PROPAGATION_REQUIRED")
                    .to("sql:UPDATE account SET balance = (SELECT balance from account where name = 'foo') - # WHERE name = 'foo'?dataSource=dataSource")
                    .to("sql:UPDATE account SET balance = (SELECT balance from account where name = 'bar') + # WHERE name = 'bar'?dataSource=dataSource")
                    .throwException(new SQLException("forced exception for test"))
                    .to("seda:transaction.outgoing.three");
            }
        };
    }

    @Override
    protected AbstractApplicationContext createApplicationContext() {
        return new ClassPathXmlApplicationContext("META-INF/spring/JdbcTransactionSampleTest-context.xml");
    }
}