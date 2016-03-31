package com.fun.bi.realtime.common;

import java.util.ArrayList;
import java.util.List;

/**
 * Represents a list of HBase columns.
 *
 * There are two types of columns, <i>standard</i> and <i>counter</i>.
 *
 * Standard columns have <i>column family</i> (required), <i>qualifier</i>
 * (optional), <i>timestamp</i> (optional), and a <i>value</i> (optional)
 * values.
 *
 * Counter columns have <i>column family</i> (required), <i>qualifier</i>
 * (optional), and an <i>increment</i> (optional, but recommended) values.
 *
 * Inserts/Updates can be added via the <code>addColumn()</code> and
 * <code>addCounter()</code> methods.
 *
 */
public class ColumnList {

	private ArrayList<Column> columns;
	private ArrayList<Counter> counters;

	public static abstract class AbstractColumn {
		byte[] family, qualifier;

		AbstractColumn(byte[] family, byte[] qualifier) {
			this.family = family;
			this.qualifier = qualifier;
		}

		public byte[] getFamily() {
			return family;
		}

		public byte[] getQualifier() {
			return qualifier;
		}

	}

	public static class Column extends AbstractColumn {
		byte[] value;

		Column(byte[] family, byte[] qualifier, byte[] value) {
			super(family, qualifier);
			this.value = value;
		}

		public byte[] getValue() {
			return value;
		}
	}

	public static class Counter extends AbstractColumn {
		long incr = 0;

		Counter(byte[] family, byte[] qualifier, long incr) {
			super(family, qualifier);
			this.incr = incr;
		}

		public long getIncrement() {
			return incr;
		}
	}

	private ArrayList<Column> columns() {
		if (this.columns == null) {
			this.columns = new ArrayList<Column>();
		}
		return this.columns;
	}

	private ArrayList<Counter> counters() {
		if (this.counters == null) {
			this.counters = new ArrayList<Counter>();
		}
		return this.counters;
	}

	/**
	 * Add a standard HBase column
	 * 
	 * @param family
	 * @param qualifier
	 * @param value
	 * @return
	 */
	public ColumnList addColumn(byte[] family, byte[] qualifier, byte[] value) {
		columns().add(new Column(family, qualifier, value));
		return this;
	}

	/**
	 * Add an HBase counter column.
	 *
	 * @param family
	 * @param qualifier
	 * @param incr
	 * @return
	 */
	public ColumnList addCounter(byte[] family, byte[] qualifier, long incr) {
		counters().add(new Counter(family, qualifier, incr));
		return this;
	}

	/**
	 * Query to determine if we have column definitions.
	 *
	 * @return
	 */
	public boolean hasColumns() {
		return this.columns != null;
	}

	/**
	 * Query to determine if we have counter definitions.
	 *
	 * @return
	 */
	public boolean hasCounters() {
		return this.counters != null;
	}

	/**
	 * Get the list of column definitions.
	 *
	 * @return
	 */
	public List<Column> getColumns() {
		return this.columns;
	}

	/**
	 * Get the list of counter definitions.
	 * 
	 * @return
	 */
	public List<Counter> getCounters() {
		return this.counters;
	}

}
