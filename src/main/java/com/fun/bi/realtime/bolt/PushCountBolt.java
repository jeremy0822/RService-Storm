package com.fun.bi.realtime.bolt;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * @author jeremy
 * @date 2016年3月8日
 * @desc
 */
public class PushCountBolt extends BaseBasicBolt {

	private static final long serialVersionUID = -2587567063222638225L;
	private static final Log log = LogFactory.getLog(PushCountBolt.class);

	private static Map<String, Long> cache = new HashMap<String, Long>();
	private static Map<String, Long> minMsgTime = new HashMap<String, Long>();
	private long lastExeTimestamp = 0;

	public void execute(Tuple input, BasicOutputCollector collector) {
		String streamId = input.getSourceStreamId();
		String kpiDM = input.getStringByField("kpiDM");
		long time = input.getLongByField("time");

		String key = streamId + "@" + kpiDM;
		putCache(key);
		putMessageTime(key, time);

		try {
			long start = System.currentTimeMillis();
			if ((start - lastExeTimestamp) > 10 * 1000) {
				for (String ky : cache.keySet()) {
					collector.emit(ky.split("@")[0], new Values(ky.split("@")[1], minMsgTime.get(ky), cache.get(ky)));
				}
				lastExeTimestamp = System.currentTimeMillis();
				log.info("[" + new SimpleDateFormat("yyyy-MM-dd:HH:mm:ss").format(new Date().getTime())
						+ "]PushCountBolt 发送消息数:" + cache.size() + " ,cost " + (lastExeTimestamp - start) + " ms.");
				cache.clear();
				minMsgTime.clear();
			}
		} catch (Exception e) {
			log.error("异常发生,收到消息【" + kpiDM + "】" + e.getMessage());
		}
	}

	private void putMessageTime(String key, long time) {
		if (minMsgTime.containsKey(key)) {
			if (time > minMsgTime.get(key)) {
				time = minMsgTime.get(key);
			}
		}
		minMsgTime.put(key, time);
	}

	private void putCache(String key) {
		if (cache.containsKey(key)) {
			cache.put(key, cache.get(key) + 1);
		} else {
			cache.put(key, 1l);
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream("reach", new Fields("rowkey", "time", "count"));
		declarer.declareStream("click", new Fields("rowkey", "time", "count"));
	}
}
