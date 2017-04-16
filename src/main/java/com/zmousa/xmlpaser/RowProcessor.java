package com.zmousa.xmlpaser;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;

public class RowProcessor implements Processor {

	@Override
	public void process(Exchange exchange) throws Exception {
		exchange.getOut().setBody(exchange.getIn().getBody());
	}
}
