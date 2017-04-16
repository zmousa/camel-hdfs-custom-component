package com.zmousa.HdfsComponent;

public class CustomHDFSConfiguration {
	private String hostName;
	private int port;
	private String path;
	public static final int DEFAULT_PORT = 8020;
	public static final String HDFS_PREFIX = "hdfs://";
	
	public String getHostName() {
		return hostName;
	}
	public void setHostName(String hostName) {
		this.hostName = hostName;
	}
	public int getPort() {
		return port;
	}
	public void setPort(int port) {
		this.port = port;
	}
	public String getPath() {
		return path;
	}
	public void setPath(String path) {
		this.path = path;
	}
}
