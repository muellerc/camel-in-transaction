package org.apache.cmueller.camel.samples.camelone.jms;

import org.apache.camel.Exchange;

public class BusinessService {

	public void computeOffer(Exchange exchange) throws Exception {
		// simulate processing...
		Thread.sleep(100);
	}
}
