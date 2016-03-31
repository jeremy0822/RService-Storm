//package com.fun.bi.realtime.spout;
//
//import java.util.Map;
//
//import org.apache.commons.logging.Log;
//import org.apache.commons.logging.LogFactory;
//
//import backtype.storm.spout.SpoutOutputCollector;
//import backtype.storm.task.TopologyContext;
//import backtype.storm.topology.OutputFieldsDeclarer;
//import backtype.storm.topology.base.BaseRichSpout;
//import backtype.storm.tuple.Fields;
//import backtype.storm.tuple.Values;
//import backtype.storm.utils.Utils;
//
//public class ProduceRecordSpout extends BaseRichSpout {
//
//     private static final long serialVersionUID = 1L;
//     private static final Log LOG = LogFactory.getLog(ProduceRecordSpout.class);
//     private SpoutOutputCollector collector;
//    
//     @Override
//     public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
//          this.collector = collector;    
//     }
//
//     @Override
//     public void nextTuple() {
//          Utils.sleep(5000);
//          String record = "1.3.0.0	28/Feb/2016:14:28:55 +0800	/ecom_mobile/fbuffer?dev=xiaomi&ver=0.2.0&fudid=7e32ca65af50ae50ad50ad5048dc85977c3c95303f945ea91ac599d01ef893cd&sid=2151&ptnr=xm&mac=3480b3a83e9d&nt=1&ih=D77F06C15A1AAE205E34827A480046C8E15A5590&ok=0&bpos=0&btm=1456727335757&ptype=0&rtm=1910&vt=1&apptype=xm_app_media&server=127.0.0.1&lian=0&stype=2&hashid=D77F06C15A1AAE205E34827A480046C8E15A5590&mid=103816&eid=4	-	Funshion-xmsdk/0.2.0 (Android/4.4.4; aphone; HM 2A)";
//          collector.emit(new Values(record));
//          LOG.info("Record emitted: record=" + record);
//     }
//
//     @Override
//     public void declareOutputFields(OutputFieldsDeclarer declarer) {
//          declarer.declare(new Fields("record"));         
//     }
//}