package org.apache.cmueller.camel.samples.camelone.jdbc;

import java.sql.SQLException;

import javax.sql.DataSource;

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

public class JdbcTransactionSampleTest extends CamelSpringTestSupport {
	
	private JdbcTemplate jdbc;
	private TransactionTemplate transactionTemplate;
	
    @Before
    @Override
    public void setUp() throws Exception {
    	super.setUp();
    	
        DataSource ds = context.getRegistry().lookup("dataSource", DataSource.class);
        jdbc = new JdbcTemplate(ds);
        
        PlatformTransactionManager transactionManager = context.getRegistry().lookup("transactionManager", PlatformTransactionManager.class);
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
	
	@Test
	public void moneyShouldBeTransfered() throws Exception {
		assertEquals(1000, queryForLong("SELECT balance from account where name = 'foo'"));
		assertEquals(1000, queryForLong("SELECT balance from account where name = 'bar'"));
		
		template.sendBody("seda:transaction.incoming.one", new Long(100));
		
		Exchange exchange = consumer.receive("seda:transaction.outgoing.one", 5000);
		assertNotNull(exchange);
		
		assertEquals(900, queryForLong("SELECT balance from account where name = 'foo'"));
		assertEquals(1100, queryForLong("SELECT balance from account where name = 'bar'"));
	}
	
	@Test
	public void moneyShouldNotTransfered() throws Exception {
		assertEquals(1000, queryForLong("SELECT balance from account where name = 'foo'"));
		assertEquals(1000, queryForLong("SELECT balance from account where name = 'bar'"));
		
		template.sendBody("seda:transaction.incoming.two", new Long(100));
		Thread.sleep(2000);
		
		assertEquals(1000, queryForLong("SELECT balance from account where name = 'foo'"));
		assertEquals(1000, queryForLong("SELECT balance from account where name = 'bar'"));
	}
	
	@Test
	public void moneyShouldNotTransfered2() throws Exception {
		assertEquals(1000, queryForLong("SELECT balance from account where name = 'foo'"));
		assertEquals(1000, queryForLong("SELECT balance from account where name = 'bar'"));
		
		template.sendBody("seda:transaction.incoming.three", new Long(100));
		Thread.sleep(2000);
		
		assertEquals(1000, queryForLong("SELECT balance from account where name = 'foo'"));
		assertEquals(1000, queryForLong("SELECT balance from account where name = 'bar'"));
	}

    @Override
    protected RouteBuilder createRouteBuilder() throws Exception {
        return new RouteBuilder() {
            @Override
            public void configure() throws Exception {
                from("seda:transaction.incoming.one")
                    .transacted("PROPAGATION_REQUIRED")
                    .to("sql:UPDATE account SET balance = (SELECT balance from account where name = 'foo') - # WHERE name = 'foo'?dataSourceRef=dataSource")
                    .to("sql:UPDATE account SET balance = (SELECT balance from account where name = 'bar') + # WHERE name = 'bar'?dataSourceRef=dataSource")
                    .to("seda:transaction.outgoing.one");
                
                from("seda:transaction.incoming.two")
	                .transacted("PROPAGATION_REQUIRED")
	                .to("sql:UPDATE account SET balance = (SELECT balance from account where name = 'foo') - # WHERE name = 'foo'?dataSourceRef=dataSource")
	                .throwException(new SQLException("forced exception for test"))
	                .to("sql:UPDATE account SET balance = (SELECT balance from account where name = 'bar') + # WHERE name = 'bar'?dataSourceRef=dataSource")
	                .to("seda:transaction.outgoing.two");
                
                from("seda:transaction.incoming.three")
	                .transacted("PROPAGATION_REQUIRED")
	                .to("sql:UPDATE account SET balance = (SELECT balance from account where name = 'foo') - # WHERE name = 'foo'?dataSourceRef=dataSource")
	                .to("sql:UPDATE account SET balance = (SELECT balance from account where name = 'bar') + # WHERE name = 'bar'?dataSourceRef=dataSource")
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