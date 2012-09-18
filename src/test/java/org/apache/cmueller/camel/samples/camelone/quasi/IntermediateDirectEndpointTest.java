package org.apache.cmueller.camel.samples.camelone.quasi;

import org.apache.camel.Exchange;
import org.apache.camel.Produce;
import org.apache.camel.ProducerTemplate;
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
import java.util.concurrent.CountDownLatch;

public class IntermediateDirectEndpointTest extends CamelSpringTestSupport
{
    @Produce(uri = "jms:input")
    private ProducerTemplate producerTemplate;

    @Override
    protected AbstractApplicationContext createApplicationContext()
    {
        return new ClassPathXmlApplicationContext("META-INF/spring/IntermediateDirectEndpointTest-context.xml");
    }

    private JdbcTemplate jdbc;
    private TransactionTemplate transactionTemplate;

    private CountDownLatch latch;

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

    @Override
    protected RouteBuilder createRouteBuilder() throws Exception
    {
        return new RouteBuilder()
        {
            @Override
            public void configure() throws Exception
            {
                from("jms:input")
                        .to("sql:UPDATE account SET balance = (SELECT balance from account where name = 'foo') - # WHERE name = 'foo'?dataSourceRef=dataSource")
                        .to("direct:intermediate");

                from("direct:intermediate")
                        .to("sql:UPDATE account SET balance = (SELECT balance from account where name = 'bar') + # WHERE name = 'bar'?dataSourceRef=dataSource")
                        .choice()
                        .when(body().isLessThan(0)).throwException(new IllegalArgumentException("Cannot transfer negative numbers!")).end()
                        .to("jms:output");
            }
        };
    }

    @Test
    public void testTransactionRollsBack()
    {
        template.sendBody("jms:input", -1);

        Exchange exchange = consumer.receive("jms:queue:ActiveMQ.DLQ", 5000);
        assertNotNull(exchange);
        assertEquals(-1, exchange.getIn().getBody(Integer.class).longValue());
        assertEquals(1000, queryForLong("SELECT balance from account where name = 'foo'"));
        assertEquals(1000, queryForLong("SELECT balance from account where name = 'bar'"));
    }
}
