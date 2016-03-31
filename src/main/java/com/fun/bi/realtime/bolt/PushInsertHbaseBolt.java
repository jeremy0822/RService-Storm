package com.fun.bi.realtime.bolt;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.util.Bytes;

import com.fun.bi.realtime.common.ColumnList;
import com.fun.bi.realtime.common.CommonUtil;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

/**
 * @author jeremy
 * @date 2016年3月9日
 * @desc 统计数据保存至hbase 单线程，设置临时缓存
 */
public class PushInsertHbaseBolt extends AbstractHBaseBolt {

	private static final long serialVersionUID = -1240366293704610657L;
	public static final Log log = LogFactory.getLog(PushInsertHbaseBolt.class);
	private static int total = 0;

	public PushInsertHbaseBolt(String tableName, byte[] family) {
		super(tableName, family);
	}

	public void execute(Tuple input, BasicOutputCollector collector) {
		String key = input.getStringByField("rowkey");
		long time = input.getLongByField("time");
		long count = input.getLongByField("count");
		byte[] rowKey = Bytes.toBytes(key);

		byte[] q1 = null;
		byte[] q2 = null;
		String streamId = input.getSourceStreamId();
		if ("reach".equals(streamId)) {
			q1 = Bytes.toBytes("reachTime");
			q2 = Bytes.toBytes("reachCount");
		} else if ("click".equals(streamId)) {
			q1 = Bytes.toBytes("clickTime");
			q2 = Bytes.toBytes("clickCount");
		}

		try {
			ColumnList cols = new ColumnList();
			cols.addColumn(family, q1, Bytes.toBytes(CommonUtil.transLongTimeToStr(time, "yyyy/MM/dd HH:mm:ss")));
			cols.addCounter(family, q2, count);
			List<Mutation> mutations = hbaseClient.constructMutationReq(rowKey, cols);
			hbaseClient.checkAndBatch(mutations, family, q1);
		} catch (Exception e) {
			log.warn("Failing tuple. Error writing rowKey " + rowKey, e);
			return;
		}
		total++;
		if (total % 10000 == 0) {
			log.info("[" + new SimpleDateFormat("yyyy-MM-dd:HH:mm:ss").format(new Date().getTime())
					+ "]PushInsertHbaseBolt 保存入库hbase数:" + total);
			total = 0;
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {

	}
}
