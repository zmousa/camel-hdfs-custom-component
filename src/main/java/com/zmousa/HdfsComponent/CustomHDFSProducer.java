package com.zmousa.HdfsComponent;

import java.io.IOException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.camel.Exchange;
import org.apache.camel.impl.DefaultProducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public final class CustomHDFSProducer extends DefaultProducer {

	private final CustomHDFSConfiguration config;
	private FSDataOutputStream ostream;
	private long hdfsLastAccess;
	private final long idleLimit = 30000;
	private volatile ScheduledExecutorService scheduler;
	
	public CustomHDFSProducer(CustomHDFSEndpoint endpoint, CustomHDFSConfiguration config) {
        super(endpoint);
        this.config = config;
    }
	
	 @Override
    protected void doStart() throws Exception {
        super.doStart();
        ostream = setupHdfs();
        scheduler = getEndpoint().getCamelContext().getExecutorServiceManager().newSingleThreadScheduledExecutor(this, "HdfsIdleCheck");
        scheduler.scheduleAtFixedRate(new IdleCheck(), idleLimit, idleLimit, TimeUnit.MILLISECONDS);
	 }

	@Override
	public void process(Exchange exchange) throws Exception {
		String body = (String)exchange.getIn().getBody();
		ostream = setupHdfs();
		if (ostream != null)
			ostream.writeUTF(body.toString());
		hdfsLastAccess = System.currentTimeMillis();
	}
	
	
	private synchronized FSDataOutputStream setupHdfs() throws Exception {
        if (ostream != null) {
            return ostream;
        }
        System.setProperty("hadoop.home.dir", "/");
        Configuration conf = new Configuration();
		conf.set("fs.defaultFS", CustomHDFSConfiguration.HDFS_PREFIX + config.getHostName());
		FileSystem fs = FileSystem.get(conf);
        Path file = new Path(CustomHDFSConfiguration.HDFS_PREFIX + config.getHostName() + config.getPath());
		if (fs.exists(file)) {
			System.out.println("File exists.");
			return null;
		} else {
			ostream = fs.create(file);
			return ostream;
		}
    }
	
	
	private final class IdleCheck implements Runnable {
        @Override
        public void run() {
            if (ostream == null) {
                return;
            }

            System.out.println("IdleCheck running");

            if (System.currentTimeMillis() - hdfsLastAccess > idleLimit) {
                try {
                	System.out.println("Closing stream as idle");
                    ostream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}