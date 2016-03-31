package com.fun.bi.realtime.bolt;

import java.util.Map;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.fun.bi.realtime.common.CacheUtil;
import com.fun.bi.realtime.common.CommonUtil;
import com.fun.bi.realtime.model.DmArea;

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
public class IncrLogsFilterBolt extends BaseBasicBolt {

	private static final long serialVersionUID = -4050821115962686971L;
	public static final Log log = LogFactory.getLog(IncrLogsFilterBolt.class);

	public void execute(Tuple input, BasicOutputCollector collector) {
		String record = input.getString(0);
		String[] dm = filter(record);
		if (ArrayUtils.isEmpty(dm)) {
			return;
		}
		String dateid = dm[0];
		String platid = dm[1];
		String areaid = dm[2];
		String hourid = dm[3]+"h";

		collector.emit(new Values(platid + "_" + dateid + "_" + areaid, hourid));
		collector.emit(new Values("-1000_" + dateid + "_" + areaid, hourid));
		collector.emit(new Values(platid + "_" + dateid + "_-1000", hourid));
		collector.emit(new Values("-1000_" + dateid + "_-1000", hourid));
	}

	/**
	 * 过滤不满足条件的日志，并且返回满足条件日志的时间和地域编码和platid
	 * 
	 * @param record
	 * @return
	 */
	private String[] filter(String record) {
		String areaid = null;
		String platid = null;
		String dateid = null;
		String hourid = null;
		try {
			String[] data = CommonUtil.strToAry(record, "\t");
			Map<String, String> paramMap = CommonUtil.parseUrlParamToMap(data[2]);
			// 过滤既没有vid也没有mid的日志
			if (!paramMap.containsKey("vid") && !paramMap.containsKey("mid")) {
				return null;
			}

			DmArea areaDM = CacheUtil.getArea(CacheUtil.ip2long(data[0]));
			areaid = areaDM.getProvinceID();
			if ("0".equals(areaDM.getProvinceID())) {
				areaid = areaDM.getCountyID();
			}

			dateid = CommonUtil.parseLogTimeToStr(data[1], "dd/MMM/yyyy:HH:mm:ss Z", "yyyyMMdd");
			hourid = CommonUtil.parseLogTimeToStr(data[1], "dd/MMM/yyyy:HH:mm:ss Z", "HH");

			String apptype = paramMap.get("apptype");
			if (apptype == null || "unknown".equals(apptype.trim().toLowerCase())) {
				platid = CacheUtil.getPlatId(CommonUtil.getPlatTypeFromDev(paramMap.get("dev"))).toString();
			} else {
				platid = CacheUtil.getPlatId(CommonUtil.getPlatType(paramMap.get("apptype"))).toString();
			}

		} catch (Exception e) {
			return null;
		}
		String[] ret = new String[] { dateid, platid, areaid, hourid };
		return ret;

	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("kpiDM", "hourid"));
	}

}
