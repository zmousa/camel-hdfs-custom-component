package com.zmousa.HdfsComponent;

import java.util.Map;

import org.apache.camel.CamelContext;
import org.apache.camel.Endpoint;
import org.apache.camel.impl.UriEndpointComponent;

public class CustomHDFSComponent extends UriEndpointComponent {
    public CustomHDFSComponent() {
        super(CustomHDFSEndpoint.class);
    }

    public CustomHDFSComponent(CamelContext context) {
        super(context, CustomHDFSEndpoint.class);
    }

    protected final Endpoint createEndpoint(String uri, String remaining, Map<String, Object> parameters) throws Exception {
    	CustomHDFSEndpoint hdfsEndpoint = new CustomHDFSEndpoint(uri, this.getCamelContext());
        setProperties(hdfsEndpoint.getConfig(), parameters);
        return hdfsEndpoint;
    }
}