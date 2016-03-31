package com.fun.bi.realtime.topology;

import org.apache.hadoop.hbase.util.Bytes;

import com.fun.bi.realtime.bolt.IncrCountBolt;
import com.fun.bi.realtime.bolt.IncrLogsFilterBolt;
import com.fun.bi.realtime.bolt.InsertHbaseBolt;
import com.fun.bi.realtime.common.ApplicationProperties;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;

/**
 * @author jeremy
 * @date 2016年3月4日
 */
public class PlayCountTopology {
	public static void main(String[] args) {
		String topic = null;
		if (args != null && args.length > 0) {
			topic = args[0];
		} else {
			System.out.println("需要参数：<kafka topic>");
			System.exit(1);
		}
		String hosts = ApplicationProperties.getProperties("zookeeper.connect");
		BrokerHosts brokerHosts = new ZkHosts(hosts);
		SpoutConfig kafkaConfig = new SpoutConfig(brokerHosts, topic, "/" + topic, "PlayCountTopology");
		kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
		KafkaSpout kafkaSpout = new KafkaSpout(kafkaConfig);
		kafkaConfig.stateUpdateIntervalMs = 1000;

		System.out.println("连接hosts：" + hosts);

		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("spout", kafkaSpout, 9);
		builder.setBolt("log.filter", new IncrLogsFilterBolt(), 6).shuffleGrouping("spout");
		builder.setBolt("log.sum", new IncrCountBolt(), 1).fieldsGrouping("log.filter", new Fields("kpiDM"));
		builder.setBolt("insertDB", new InsertHbaseBolt("f_realtime_play_analyse", Bytes.toBytes("f")), 1)
				.shuffleGrouping("log.sum");
		Config config = new Config();

		try {
			// for test
			// config.setMaxTaskParallelism(3);
			// LocalCluster cluster = new LocalCluster();
			// cluster.submitTopology(topic, config, builder.createTopology());
			// Thread.sleep(20000);
			// cluster.shutdown();

			config.setNumAckers(0);
			config.setNumWorkers(1);
			config.setDebug(false);
			StormSubmitter.submitTopology("MobilePlayCounterTopolgy", config, builder.createTopology());
		} catch (Exception e) {
			System.err.println(e.getMessage());
		}
	}
}
