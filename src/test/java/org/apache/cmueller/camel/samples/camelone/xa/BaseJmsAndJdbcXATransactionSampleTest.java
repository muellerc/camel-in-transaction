package org.apache.cmueller.camel.samples.camelone.xa;

import java.sql.SQLException;
import java.util.concurrent.CountDownLatch;

import javax.sql.DataSource;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.test.junit4.CamelSpringTestSupport;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionCallbackWithoutResult;
import org.springframework.transaction.support.TransactionTemplate;

public abstract class BaseJmsAndJdbcXATransactionSampleTest extends CamelSpringTestSupport {

    private JdbcTemplate jdbc;
    private TransactionTemplate transactionTemplate;

    private CountDownLatch latch = new CountDownLatch(1000);

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();

        DataSource ds = context.getRegistry().lookup("dataSource", DataSource.class);
        jdbc = new JdbcTemplate(ds);

        PlatformTransactionManager transactionManager = context.getRegistry().lookup("jtaTransactionManager", PlatformTransactionManager.class);
        transactionTemplate = new TransactionTemplate(transactionManager);

        transactionTemplate.execute(new TransactionCallbackWithoutResult() {
            @Override
            protected void doInTransactionWithoutResult(TransactionStatus status) {
                jdbc.execute("CREATE TABLE account (name VARCHAR(50), balance BIGINT)");
                jdbc.execute("INSERT INTO account VALUES('foo',1000)");
                jdbc.execute("INSERT INTO account VALUES('bar',1000)");
            }
        });
    }

    @After
    @Override
    public void tearDown() throws Exception {
        transactionTemplate.execute(new TransactionCallbackWithoutResult() {
            @Override
            protected void doInTransactionWithoutResult(TransactionStatus status) {
                jdbc.execute("DROP TABLE account");
            }
        });

        super.tearDown();
    }

    private long queryForLong(final String query) {
        return transactionTemplate.execute(new TransactionCallback<Long>() {
            @Override
            public Long doInTransaction(TransactionStatus status) {
                return jdbc.queryForLong(query);
            }
        });
    }

    @Ignore
    @Test
    public void moneyShouldBeTransfered() {
        assertEquals(1000, queryForLong("SELECT balance from account where name = 'foo'"));
        assertEquals(1000, queryForLong("SELECT balance from account where name = 'bar'"));

        template.sendBody("activemq:queue:transaction.incoming.one", new Long(100));

        Exchange exchange = consumer.receive("activemq:queue:transaction.outgoing.one", 5000);
        assertNotNull(exchange);

        assertEquals(900, queryForLong("SELECT balance from account where name = 'foo'"));
        assertEquals(1100, queryForLong("SELECT balance from account where name = 'bar'"));
    }
    @Ignore
    @Test
    public void moneyShouldNotTransfered() {
        assertEquals(1000, queryForLong("SELECT balance from account where name = 'foo'"));
        assertEquals(1000, queryForLong("SELECT balance from account where name = 'bar'"));

        template.sendBody("activemq:queue:transaction.incoming.two", new Long(100));

        Exchange exchange = consumer.receive("activemq:queue:ActiveMQ.DLQ", 5000);
        assertNotNull(exchange);

        assertEquals(1000, queryForLong("SELECT balance from account where name = 'foo'"));
        assertEquals(1000, queryForLong("SELECT balance from account where name = 'bar'"));
    }

    @Ignore
    @Test
    public void moneyShouldNotTransfered2() throws Exception {
        assertEquals(1000, queryForLong("SELECT balance from account where name = 'foo'"));
        assertEquals(1000, queryForLong("SELECT balance from account where name = 'bar'"));

        template.sendBody("activemq:queue:transaction.incoming.three", new Long(100));

        Exchange exchange = consumer.receive("activemq:queue:ActiveMQ.DLQ", 5000);
        assertNotNull(exchange);

        assertEquals(1000, queryForLong("SELECT balance from account where name = 'foo'"));
        assertEquals(1000, queryForLong("SELECT balance from account where name = 'bar'"));
    }

    @Test
    public void perfTest() throws Exception {
        assertEquals(1000, queryForLong("SELECT balance from account where name = 'foo'"));
        assertEquals(1000, queryForLong("SELECT balance from account where name = 'bar'"));

        long start = System.currentTimeMillis();
        for (int i = 0; i < 1000; i++) {
            template.sendBody("activemq:queue:transaction.incoming.four", new Long(1));
        }
        latch.await();
        long end = System.currentTimeMillis();

        System.out.println("duration: " + (end -start) + "ms");

        assertEquals(0, queryForLong("SELECT balance from account where name = 'foo'"));
        assertEquals(2000, queryForLong("SELECT balance from account where name = 'bar'"));
    }

    @Override
    protected RouteBuilder createRouteBuilder() throws Exception {
        return new RouteBuilder() {
            @Override
            public void configure() throws Exception {
                from("activemqXa:queue:transaction.incoming.one")
                    .transacted("PROPAGATION_REQUIRED")
                    .to("sql:UPDATE account SET balance = (SELECT balance from account where name = 'foo') - # WHERE name = 'foo'?dataSourceRef=dataSource")
                    .to("sql:UPDATE account SET balance = (SELECT balance from account where name = 'bar') + # WHERE name = 'bar'?dataSourceRef=dataSource")
                    .to("activemqXa:queue:transaction.outgoing.one");

                from("activemqXa:queue:transaction.incoming.two")
                    .transacted("PROPAGATION_REQUIRED")
                    .to("sql:UPDATE account SET balance = (SELECT balance from account where name = 'foo') - # WHERE name = 'foo'?dataSourceRef=dataSource")
                    .throwException(new SQLException("forced exception for test"))
                    .to("sql:UPDATE account SET balance = (SELECT balance from account where name = 'bar') + # WHERE name = 'bar'?dataSourceRef=dataSource")
                    .to("activemqXa:queue:transaction.outgoing.two");

                from("activemqXa:queue:transaction.incoming.three")
                    .transacted("PROPAGATION_REQUIRED")
                    .to("sql:UPDATE account SET balance = (SELECT balance from account where name = 'foo') - # WHERE name = 'foo'?dataSourceRef=dataSource")
                    .to("sql:UPDATE account SET balance = (SELECT balance from account where name = 'bar') + # WHERE name = 'bar'?dataSourceRef=dataSource")
                    .throwException(new SQLException("forced exception for test"))
                    .to("activemqXa:queue:transaction.outgoing.three");

                from("activemqXa:queue:transaction.incoming.four?concurrentConsumers=5")
                .transacted("PROPAGATION_REQUIRED")
                .to("sql:UPDATE account SET balance = balance - # WHERE name = 'foo'?dataSourceRef=dataSource")
                .to("sql:UPDATE account SET balance = balance + # WHERE name = 'bar'?dataSourceRef=dataSource")
                .to("activemqXa:queue:transaction.outgoing.four")
                .process(new Processor() {
                                 @Override
                                 public void process(Exchange exchange) throws Exception {
                                        latch.countDown();
                                 }
                           });
            }
        };
    }
}