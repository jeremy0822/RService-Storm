package com.fun.bi.realtime.bolt;

import java.util.Map;

import org.apache.commons.lang.Validate;

import com.fun.bi.realtime.common.HBaseClient;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.base.BaseBasicBolt;

public abstract class AbstractHBaseBolt extends BaseBasicBolt {
	private static final long serialVersionUID = -1647584635866221274L;

	protected String tableName;
	protected byte[] family;
	protected HBaseClient hbaseClient;

	public AbstractHBaseBolt(String tableName, byte[] family) {
		Validate.notEmpty(tableName, "Table name can not be blank or null");
		this.tableName = tableName;
		this.family = family;
	}

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context) {
		super.prepare(stormConf, context);
		hbaseClient = new HBaseClient(tableName);
	}

}
