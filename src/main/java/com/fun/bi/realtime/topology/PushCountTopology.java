package com.fun.bi.realtime.topology;

import org.apache.hadoop.hbase.util.Bytes;

import com.fun.bi.realtime.bolt.PushCountBolt;
import com.fun.bi.realtime.bolt.PushInsertHbaseBolt;
import com.fun.bi.realtime.bolt.PushLogFilterBolt;
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
public class PushCountTopology {
	public static void main(String[] args) {
		String topicreach = null;
		String topiclick = null;
		if (args != null && args.length == 2) {
			topicreach = args[0];
			topiclick = args[1];
		} else {
			System.out.println("需要参数：<kafka topic pushreach><kafka topic push click>");
			System.exit(1);
		}
		String hosts = ApplicationProperties.getProperties("zookeeper.connect");
		System.out.println("zookeeper.connect.host：" + hosts);
		BrokerHosts brokerHosts = new ZkHosts(hosts);

		KafkaSpout kafkaSpout = newKafkaSpout(brokerHosts, topicreach, "pushreach");
		KafkaSpout kafkaSpout2 = newKafkaSpout(brokerHosts, topiclick, "pushclick");

		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("spout.reach", kafkaSpout, 9);
		builder.setSpout("spout.click", kafkaSpout2, 9);

		builder.setBolt("log.filter", new PushLogFilterBolt(), 8).shuffleGrouping("spout.reach")
				.shuffleGrouping("spout.click");
		builder.setBolt("log.sum", new PushCountBolt(), 1).fieldsGrouping("log.filter", "reach", new Fields("kpiDM"))
				.fieldsGrouping("log.filter", "click", new Fields("kpiDM"));
		builder.setBolt("insertDB", new PushInsertHbaseBolt("f_realtime_push_analyse", Bytes.toBytes("f")), 1)
				.shuffleGrouping("log.sum", "reach").shuffleGrouping("log.sum", "click");

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
			StormSubmitter.submitTopology("PushCountTopology", config, builder.createTopology());
		} catch (Exception e) {
			System.err.println(e.getMessage());
		}
	}

	private static KafkaSpout newKafkaSpout(BrokerHosts brokerHosts, String topic, String id) {
		SpoutConfig kafkaConfig = new SpoutConfig(brokerHosts, topic, "/" + topic, id);
		kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
		KafkaSpout kafkaSpout = new KafkaSpout(kafkaConfig);
		kafkaConfig.stateUpdateIntervalMs = 1000;
		return kafkaSpout;
	}
}
