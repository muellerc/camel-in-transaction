package org.apache.cmueller.camel.samples.camelone.quasi;

import org.apache.camel.Exchange;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.test.junit4.CamelSpringTestSupport;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionCallbackWithoutResult;
import org.springframework.transaction.support.TransactionTemplate;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.Arrays;

public class JmsAndJdbcQuasiTransactionSampleTest extends CamelSpringTestSupport
{
    private JdbcTemplate jdbc;
    private TransactionTemplate transactionTemplate;
//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    @Override
    protected AbstractApplicationContext createApplicationContext()
    {
        return new ClassPathXmlApplicationContext("META-INF/spring/JmsAndJdbcQuasiTransactionSampleTest-context.xml");
    }

    @Before
    @Override
    public void setUp() throws Exception
    {
        super.setUp();

        DataSource ds = context.getRegistry().lookup("dataSource", DataSource.class);
        jdbc = new JdbcTemplate(ds);

        PlatformTransactionManager transactionManager = context.getRegistry().lookup("transactionManager", PlatformTransactionManager.class);
        transactionTemplate = new TransactionTemplate(transactionManager);

        transactionTemplate.execute(new TransactionCallbackWithoutResult()
        {
            @Override
            protected void doInTransactionWithoutResult(TransactionStatus status)
            {
                jdbc.execute("CREATE TABLE account (name VARCHAR(50), balance BIGINT)");
                jdbc.execute("INSERT INTO account VALUES('foo',1000)");
                jdbc.execute("INSERT INTO account VALUES('bar',1000)");
            }
        });
    }

    @After
    @Override
    public void tearDown() throws Exception
    {
        transactionTemplate.execute(new TransactionCallbackWithoutResult()
        {
            @Override
            protected void doInTransactionWithoutResult(TransactionStatus status)
            {
                jdbc.execute("DROP TABLE account");
            }
        });

        super.tearDown();
    }


    @Override
    protected RouteBuilder createRouteBuilder() throws Exception
    {
        return new RouteBuilder()
        {
            @Override
            public void configure() throws Exception
            {
                from("jms:input.one")
                        .transacted()
                        .to("sql:UPDATE account SET balance = (SELECT balance from account where name = 'foo') - # WHERE name = 'foo'?dataSourceRef=dataSource")
                        .to("sql:UPDATE account SET balance = (SELECT balance from account where name = 'bar') + # WHERE name = 'bar'?dataSourceRef=dataSource")
                        .to("jms:output.one");

                from("jms:input.two")
                        .transacted()
                        .to("sql:UPDATE account SET balance = (SELECT balance from account where name = 'foo') - # WHERE name = 'foo'?dataSourceRef=dataSource")
                        .throwException(new SQLException("forced exception for test"))
                        .to("sql:UPDATE account SET balance = (SELECT balance from account where name = 'bar') + # WHERE name = 'bar'?dataSourceRef=dataSource")
                        .to("jms:output.two");

                from("jms:input.three")
                        .transacted()
                        .to("sql:UPDATE account SET balance = (SELECT balance from account where name = 'foo') - # WHERE name = 'foo'?dataSourceRef=dataSource")
                        .to("sql:UPDATE account SET balance = (SELECT balance from account where name = 'bar') + # WHERE name = 'bar'?dataSourceRef=dataSource")
                        .throwException(new SQLException("forced exception for test"))
                        .to("jms:output.three");

                from("direct:splitMe")
                        .transacted()
                        .split().body().shareUnitOfWork()
                        .choice()
                            .when(body().isEqualTo("poison"))
                                .throwException(new IllegalArgumentException("No way, Jose!"))
                        .to("jms:output:splat");

            }
        };
    }

    private long queryForLong(final String query)
    {
        return transactionTemplate.execute(new TransactionCallback<Long>()
        {
            @Override
            public Long doInTransaction(TransactionStatus status)
            {
                return jdbc.queryForLong(query);
            }
        });
    }


    @Test
    public void moneyShouldTransfer()
    {
        assertEquals(1000, queryForLong("SELECT balance from account where name = 'foo'"));
        assertEquals(1000, queryForLong("SELECT balance from account where name = 'bar'"));

        template.sendBody("jms:input.one", 100L);

        Exchange exchange = consumer.receive("jms:output.one", 5000);
        assertNotNull(exchange);

        assertEquals(900, queryForLong("SELECT balance from account where name = 'foo'"));
        assertEquals(1100, queryForLong("SELECT balance from account where name = 'bar'"));
    }

    @Test
    public void moneyShouldNotTransferWhenExceptionInBetweenUpdates()
    {
        assertEquals(1000, queryForLong("SELECT balance from account where name = 'foo'"));
        assertEquals(1000, queryForLong("SELECT balance from account where name = 'bar'"));

        template.sendBody("jms:input.two", 100L);

        Exchange exchange = consumer.receive("jms:ActiveMQ.DLQ", 5000);
        assertNotNull(exchange);

        assertEquals(1000, queryForLong("SELECT balance from account where name = 'foo'"));
        assertEquals(1000, queryForLong("SELECT balance from account where name = 'bar'"));
    }

    @Test
    public void moneyShouldNotTransferWhenExceptionAfterUpdates()
    {
        assertEquals(1000, queryForLong("SELECT balance from account where name = 'foo'"));
        assertEquals(1000, queryForLong("SELECT balance from account where name = 'bar'"));

        template.sendBody("jms:input.three", 100L);

        Exchange exchange = consumer.receive("jms:ActiveMQ.DLQ", 5000);
        assertNotNull(exchange);

        assertEquals(1000, queryForLong("SELECT balance from account where name = 'foo'"));
        assertEquals(1000, queryForLong("SELECT balance from account where name = 'bar'"));
    }

    @Test
    public void allSplitsShouldBeDelivered()
    {
        template.sendBody("direct:splitMe", Arrays.asList("a", "b", "c"));

        Exchange exchange1 = consumer.receive("jms:output:splat", 3000);
        assertNotNull(exchange1);
        assertEquals("a", exchange1.getIn().getBody(String.class));
        Exchange exchange2 = consumer.receive("jms:output:splat", 3000);
        assertNotNull(exchange2);
        assertEquals("b", exchange2.getIn().getBody(String.class));
        Exchange exchange3 = consumer.receive("jms:output:splat", 3000);
        assertNotNull(exchange3);
        assertEquals("c", exchange3.getIn().getBody(String.class));
    }

    @Test
    public void noSplitsShouldBeDelivered()
    {
        template.sendBody("direct:splitMe", Arrays.asList("that", "girl", "is", "poison"));
        Exchange exchange = consumer.receive("jms:output:splat", 3000);
        assertNull(exchange);
    }


}