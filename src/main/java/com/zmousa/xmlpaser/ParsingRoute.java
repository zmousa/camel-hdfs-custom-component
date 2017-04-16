package com.zmousa.xmlpaser;

import org.apache.camel.builder.RouteBuilder;
import org.springframework.beans.factory.annotation.Autowired;

public class ParsingRoute extends RouteBuilder {
	
	@Autowired
	private RowProcessor rowProcessor;

	@Override
	public void configure() {
		from("file:inputFile?moveFailed=.error")
		.process(rowProcessor)
		.to("chdfs://hdfsHost:port/hdfsPath");
    }
}