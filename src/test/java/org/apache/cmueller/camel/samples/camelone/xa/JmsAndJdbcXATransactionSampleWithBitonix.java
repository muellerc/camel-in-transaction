package org.apache.cmueller.camel.samples.camelone.xa;

import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

public class JmsAndJdbcXATransactionSampleWithBitonix extends BaseJmsAndJdbcXATransactionSample {

	@Override
	protected AbstractApplicationContext createApplicationContext() {
		return new ClassPathXmlApplicationContext("META-INF/spring/JmsAndJdbcXATransactionSampleWithBitronix-context.xml");
	}	
}