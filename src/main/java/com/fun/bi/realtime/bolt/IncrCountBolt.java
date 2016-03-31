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
 * @date 2016年2月29日
 * @desc
 */
public class IncrCountBolt extends BaseBasicBolt {

	private static final long serialVersionUID = -2587567063222638225L;
	private static final Log log = LogFactory.getLog(IncrCountBolt.class);
	private static Map<String, Long> cache = new HashMap<String, Long>();
	private long lastExeTimestamp = 0;

	public void execute(Tuple input, BasicOutputCollector collector) {
		String kpiDM = input.getStringByField("kpiDM");
		String hourid = input.getStringByField("hourid");
		String key = hourid + "@" + kpiDM;
		putCache(key);
		try {
			long start = System.currentTimeMillis();
			if ((start - lastExeTimestamp) > 10 * 1000) {
				for (String ky : cache.keySet()) {
					collector.emit(new Values(ky.split("@")[1], ky.split("@")[0], cache.get(ky)));
				}
				lastExeTimestamp = System.currentTimeMillis();
				log.info("[" + new SimpleDateFormat("yyyy-MM-dd:HH:mm:ss").format(new Date().getTime())
						+ "]IncrCountBolt 发送消息数:" + cache.size() + " ,cost " + (lastExeTimestamp - start) + " ms.");
				cache.clear();
			}
		} catch (Exception e) {
			log.error("异常发生,收到消息【" + kpiDM + "】" + e.getMessage());
		}
	}

	private void putCache(String key) {
		if (cache.containsKey(key)) {
			cache.put(key, cache.get(key) + 1);
		} else {
			cache.put(key, 1l);
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("rowkey", "hourid", "count"));
	}
}
