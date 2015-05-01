package org.apache.cmueller.camel.samples.camelone.xa;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

public class JmsAndJdbcXATransactionSampleWithJOTMTest extends BaseJmsAndJdbcXATransactionSampleTest {

    @BeforeClass
    public static void setUpBeforeClass() {
        System.setProperty("jotm.home", "src/test/resources");
    }

    @AfterClass
    public static void tearDownAfterClass() {
        System.clearProperty("jotm.home");
    }

    @Override
    protected AbstractApplicationContext createApplicationContext() {
        return new ClassPathXmlApplicationContext("META-INF/spring/JmsAndJdbcXATransactionSampleWithJOTMTest-context.xml");
    }
}