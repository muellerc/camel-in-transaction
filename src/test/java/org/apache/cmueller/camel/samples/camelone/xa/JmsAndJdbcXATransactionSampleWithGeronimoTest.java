package org.apache.cmueller.camel.samples.camelone.xa;

import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

public class JmsAndJdbcXATransactionSampleWithGeronimoTest extends BaseJmsAndJdbcXATransactionSampleTest {

    @Override
    protected AbstractApplicationContext createApplicationContext() {
        return new ClassPathXmlApplicationContext("META-INF/spring/JmsAndJdbcXATransactionSampleWithGeronimoTest-context.xml");
    }
}