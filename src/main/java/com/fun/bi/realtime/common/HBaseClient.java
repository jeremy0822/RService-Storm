package com.fun.bi.realtime.common;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;

import com.google.common.collect.Lists;

/**
 * @author jeremy
 * @date 2016年3月1日
 * @desc
 */

public class HBaseClient {
	public static final Log log = LogFactory.getLog(HBaseClient.class);

	private HTableInterface table = null;

	public HBaseClient(String tableName) {
		try {
			table = HbaseUtil.getTable(tableName);
			table.setAutoFlush(false, true);
		} catch (Exception e) {
			log.error("初始化tablename error " + e.getMessage());
		}
	}

	public List<Mutation> constructMutationReq(byte[] rowKey, ColumnList cols) {
		List<Mutation> mutations = Lists.newArrayList();

		if (cols.hasColumns()) {
			Put put = new Put(rowKey);
			for (ColumnList.Column col : cols.getColumns()) {
				put.add(col.getFamily(), col.getQualifier(), col.getValue());
			}
			mutations.add(put);
		}

		if (cols.hasCounters()) {
			Increment inc = new Increment(rowKey);
			for (ColumnList.Counter cnt : cols.getCounters()) {
				inc.addColumn(cnt.getFamily(), cnt.getQualifier(), cnt.getIncrement());
			}
			mutations.add(inc);
		}

		if (mutations.isEmpty()) {
			mutations.add(new Put(rowKey));
		}
		return mutations;
	}

	public void batch(List<Mutation> mutations) throws Exception {
		try {
			for (Mutation in : mutations) {
				if (in instanceof Put) {
					table.put((Put) in);
				} else if (in instanceof Increment) {
					table.increment((Increment) in);
				}
			}
			table.flushCommits();
		} catch (Exception e) {
			log.warn("Error performing a mutation to HBase.", e);
			throw e;
		}
	}

	public void checkAndBatch(List<Mutation> mutations,byte[] family, byte[] qualifier) throws Exception {
		try {
			for (Mutation in : mutations) {
				if (in instanceof Put) {
					table.checkAndPut(in.getRow(), family, qualifier, null, (Put) in);
				} else if (in instanceof Increment) {
					table.increment((Increment) in);
				}
			}
			table.flushCommits();
		} catch (Exception e) {
			log.warn("Error performing a mutation to HBase.", e);
			throw e;
		}
	}
}
