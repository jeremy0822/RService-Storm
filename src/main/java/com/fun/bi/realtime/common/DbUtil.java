package com.fun.bi.realtime.common;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * @author jeremy
 * 
 *         tool for connect mysql db
 */
public class DbUtil {
	private static final Log log = LogFactory.getLog(DbUtil.class);

	private static String driverName = ApplicationProperties.getProperties("connection.driver.name");
	private static String url = ApplicationProperties.getProperties("connection.url");
	private static String user = ApplicationProperties.getProperties("connection.username");
	private static String password = ApplicationProperties.getProperties("connection.password");

	public static Connection getConnection() throws Exception {
		Connection conn = null;
		try {
			Properties connectionProps = new Properties();
			connectionProps.put("user", user);
			connectionProps.put("password", password);

			Class.forName(driverName);

			conn = DriverManager.getConnection(url, connectionProps);
		} catch (Exception e) {
			log.error("初始化mysql数据连接失败！" + e.getMessage());
		}
		return conn;
	}

	public static ResultSet executeQuery(Connection con, String sql) {
		ResultSet rs = null;
		Statement stmt = null;
		try {
			stmt = con.createStatement();
			rs = stmt.executeQuery(sql);
		} catch (SQLException e) {
			log.error("sql[" + sql + "]执行出错", e);
		}
		return rs;
	}

	/**
	 * 关闭连接，并设置此连接为自动提交模式
	 * 
	 * @param con
	 * @author hujq 2014年8月28日下午5:18:30
	 */
	public static void close(Connection con) {
		try {
			if (con != null && !con.isClosed()) {
				if (!con.getAutoCommit()) {
					con.setAutoCommit(true);
				}

				con.close();
			}
		} catch (SQLException e) {
			log.error("con关闭出错", e);
		}
	}

	public static void close(Statement stmt) {
		try {
			if (stmt != null && !stmt.isClosed())
				stmt.close();
		} catch (SQLException e) {
			log.error("stmt关闭出错", e);
		}
	}

	public static void close(PreparedStatement ps) {
		try {
			if (ps != null && !ps.isClosed())
				ps.close();
		} catch (SQLException e) {
			log.error("ps关闭出错", e);
		}
	}

	public static void close(ResultSet rs) {
		try {
			if (rs != null) {
				rs.close();
				rs = null;
			}
		} catch (SQLException e) {
			log.error("rs关闭出错", e);
		}
	}
}
