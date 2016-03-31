package com.fun.bi.realtime.common;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;

/**
 * @author jeremy
 * @date 2015年12月15日
 */
public class HbaseUtil {
	
	private static HConnection pool = null;
	
	public synchronized static HConnection getHConnection(String zkhost) throws IOException {
		if (pool == null) {
			Configuration config = HBaseConfiguration.create();
			config.set("hbase.zookeeper.quorum",zkhost);
			pool = HConnectionManager.createConnection(config);
		}

		return pool;
	}
	
	
	public synchronized static HConnection getHConnection() throws IOException{
		if(pool==null){
			Configuration config = HBaseConfiguration.create();
			pool=HConnectionManager.createConnection(config);
		}
		
		return pool;
	}
	
	/**
	 * 提供定制configration的接口，不要频繁调用，会造成HConnection丢失
	 * @param config
	 * @throws IOException
	 */
	public static void initHConnection(Configuration config){
		try {
			pool = HConnectionManager.createConnection(config);
		} catch (IOException e) {
			throw new RuntimeException(e.getMessage(),e);
		}
	}
	
	public static HTableInterface getTable(String tableName){
		try {
			return getHConnection().getTable(tableName);
		} catch (IOException e) {
			throw new RuntimeException(e.getMessage(),e);
		}
	}
	
	public static HTableInterface getTable(byte[] tableName){
		try {
			return getHConnection().getTable(tableName);
		} catch (IOException e) {
			throw new RuntimeException(e.getMessage(),e);
		}
	}
	
	public static void main(String[] args) throws IOException {
		getHConnection("funshion-hadoop183:2181");
		HTableInterface table = getTable("f_search_word_analyse");
		table.close();
	}

}
