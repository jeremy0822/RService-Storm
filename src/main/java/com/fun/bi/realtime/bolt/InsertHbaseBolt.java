package com.fun.bi.realtime.bolt;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.util.Bytes;

import com.fun.bi.realtime.common.ColumnList;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

/**
 * @author jeremy
 * @date 2016年2月29日
 * @desc 统计数据保存至hbase 单线程，设置临时缓存
 */
public class InsertHbaseBolt extends AbstractHBaseBolt {

	private static final long serialVersionUID = -1240366293704610657L;
	public static final Log log = LogFactory.getLog(InsertHbaseBolt.class);
	private static int total = 0;

	public InsertHbaseBolt(String tableName, byte[] family) {
		super(tableName, family);
	}

	public void execute(Tuple input, BasicOutputCollector collector) {
		String key = input.getStringByField("rowkey");
		String hourid = input.getStringByField("hourid");
		long count = input.getLongByField("count");
		byte[] rowKey = Bytes.toBytes(key);

		ColumnList cols = new ColumnList();
		cols.addCounter(family, Bytes.toBytes(hourid), count);
		List<Mutation> mutations = hbaseClient.constructMutationReq(rowKey, cols);
		try {
			hbaseClient.batch(mutations);
		} catch (Exception e) {
			log.warn("Failing tuple. Error writing rowKey " + rowKey, e);
			return;
		}
		total++;
		if (total % 10000 == 0) {
			log.info("[" + new SimpleDateFormat("yyyy-MM-dd:HH:mm:ss").format(new Date().getTime())
					+ "]InsertHbaseBolt 保存入库hbase数:" + total);
			count = 0;
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {

	}
}
