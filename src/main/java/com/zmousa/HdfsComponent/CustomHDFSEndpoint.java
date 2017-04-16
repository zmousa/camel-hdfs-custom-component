package com.zmousa.HdfsComponent;

import java.net.URI;
import java.net.URISyntaxException;

import org.apache.camel.CamelContext;
import org.apache.camel.Consumer;
import org.apache.camel.Processor;
import org.apache.camel.Producer;
import org.apache.camel.impl.ScheduledPollEndpoint;
import org.apache.camel.spi.UriEndpoint;
import org.apache.camel.spi.UriParam;

@UriEndpoint(scheme = "chdfs", title = "HDFS", syntax = "chdfs:hostName:port/path", consumerClass = CustomHDFSProducer.class, label = "hadoop,file")
public class CustomHDFSEndpoint extends ScheduledPollEndpoint {

    @UriParam
    private final CustomHDFSConfiguration config;

    @SuppressWarnings("deprecation")
    public CustomHDFSEndpoint(String endpointUri, CamelContext context) throws URISyntaxException {
        super(endpointUri, context);
        this.config = new CustomHDFSConfiguration();
        URI uri = new URI(endpointUri);
        String protocol = uri.getScheme();
        if (!protocol.equalsIgnoreCase("chdfs")) {
            throw new IllegalArgumentException("Unrecognized protocol: " + protocol + " for uri: " + endpointUri);
        }
        config.setHostName(uri.getHost());
        config.setPort(uri.getPort() == -1 ? CustomHDFSConfiguration.DEFAULT_PORT : uri.getPort());
        config.setPath(uri.getPath());
    }

    @Override
    public Consumer createConsumer(Processor processor) throws Exception {
        return null;
    }

    @Override
    public boolean isSingleton() {
        return true;
    }

    public CustomHDFSConfiguration getConfig() {
        return config;
    }

	@Override
	public Producer createProducer() throws Exception {
        return new CustomHDFSProducer(this, config);
	}
}