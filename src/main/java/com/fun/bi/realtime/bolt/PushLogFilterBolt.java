package com.fun.bi.realtime.bolt;

import java.util.Map;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.util.StringUtils;

import com.fun.bi.realtime.common.CacheUtil;
import com.fun.bi.realtime.common.CommonUtil;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * @author jeremy
 * @date 2016年3月4日
 * @desc
 */
public class PushLogFilterBolt extends BaseBasicBolt {

	private static final long serialVersionUID = -4050821115962686971L;
	public static final Log log = LogFactory.getLog(PushLogFilterBolt.class);

	public void execute(Tuple input, BasicOutputCollector collector) {
		String record = input.getString(0);
		Object[] dm = filter(record);
		if (ArrayUtils.isEmpty(dm)) {
			return;
		}
		// String dateid = dm[0].toString();
		String platid = dm[1].toString();
		String messageid = dm[2].toString();
		long timeid = (long) dm[3];

		String componentId = input.getSourceComponent();
		if ("spout.reach".equals(componentId)) {
			collector.emit("reach", new Values(platid + "_" + messageid, timeid));
			collector.emit("reach", new Values("-1000_" + messageid, timeid));
			collector.emit("reach", new Values(platid + "_-1000", timeid));
			collector.emit("reach", new Values("-1000_-1000", timeid));
		} else if ("spout.click".equals(componentId)) {
			collector.emit("click", new Values(platid + "_" + messageid, timeid));
			collector.emit("click", new Values("-1000_" + messageid, timeid));
			collector.emit("click", new Values(platid + "_-1000", timeid));
			collector.emit("click", new Values("-1000_-1000", timeid));
		}
	}

	/**
	 * @param record
	 * @return dateid、platid、messageid、timeid
	 */
	private Object[] filter(String record) {
		String messageid = null;
		String platid = null;
		String dateid = null;
		long timeid = 0;
		try {
			String[] data = CommonUtil.strToAry(record, "\t");
			dateid = CommonUtil.parseLogTimeToStr(data[1], "dd/MMM/yyyy:HH:mm:ss Z", "yyyyMMdd");
			String timeStr = CommonUtil.parseLogTimeToStr(data[1], "dd/MMM/yyyy:HH:mm:ss Z", "yyyy/MM/dd HH:mm:ss");
			timeid = CommonUtil.transTimeToLong(timeStr, "yyyy/MM/dd HH:mm:ss");

			Map<String, String> paramMap = CommonUtil.parseUrlParamToMap(data[2]);

			String ok = paramMap.get("ok");
			if (!"1".equals(ok) && record.contains("pushreach")) {
				return null;
			}

			messageid = paramMap.get("messageid");
			if (!StringUtils.isEmpty(messageid)) {
				messageid = messageid.replace("mid:", "");// 有上报不规则的,比如：messageid=mid:10034
			}

			String apptype = paramMap.get("apptype");
			if (apptype == null || "unknown".equals(apptype.trim().toLowerCase())) {
				platid = CacheUtil.getPlatId(CommonUtil.getPlatTypeFromDev(paramMap.get("dev"))).toString();
			} else {
				platid = CacheUtil.getPlatId(CommonUtil.getPlatType(paramMap.get("apptype"))).toString();
			}
		} catch (Exception e) {
			return null;
		}
		if (StringUtils.isEmpty(messageid) || !CommonUtil.isNumber(messageid)) {
			return null;
		}
		Object[] ret = new Object[] { dateid, platid, messageid, timeid };
		return ret;

	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream("reach", new Fields("kpiDM", "time"));
		declarer.declareStream("click", new Fields("kpiDM", "time"));
	}

	public static void main(String[] args) throws Exception {
		Map<String, String> paramMap = CommonUtil.parseUrlParamToMap("/ecom_mobile/pushclick?dev=iPad_8.4.1_iPad4.2&mac=020000000000&ver=2.0.12.2&nt=1&fudid=8283F1833AAD39AD3BAD3BADD4EF36FE5998599F5DCF099B039C59CB02C80A9C&sid=0&messageid=10056&videotype=1&action=1&apptype=ipad_app_main&rprotocol=");
		String platid="";
		String apptype = paramMap.get("apptype");
		if (apptype == null || "unknown".equals(apptype.trim().toLowerCase())) {
			platid = CacheUtil.getPlatId(CommonUtil.getPlatTypeFromDev(paramMap.get("dev"))).toString();
		} else {
			platid = CacheUtil.getPlatId(CommonUtil.getPlatType(paramMap.get("apptype"))).toString();
		}
		System.out.println(platid);
		String parseLogTimeToStr = CommonUtil.parseLogTimeToStr("08/Mar/2016:17:31:12 +0800", "dd/MMM/yyyy:HH:mm:ss Z",
				"yyyy/MM/dd HH:mm:ss");
		System.out.println(parseLogTimeToStr);
		System.out.println(CommonUtil.transTimeToLong("2016/03/08 17:31:12", "yyyy/MM/dd HH:mm:ss"));
		long d = 1457494371000l;
		System.out.println(CommonUtil.transLongTimeToStr(d, "yyyy/MM/dd HH:mm:ss"));
		System.out.println(CommonUtil.transTimeToLong(parseLogTimeToStr, "yyyy/MM/dd HH:mm:ss"));
		String msg = "mid:10034";
		msg = msg.replace("mid:", "");
		System.out.println(msg);
	}
}
