package util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import config.Config;

public class HiveUtil {

	private static String driverName = "org.apache.hadoop.hive.jdbc.HiveDriver";
	private static String url = Config.HIVE_JDBC_ADDRESS;
	private static String user = "";
	private static String password = "";
	private static Connection conn = null;
	private static Statement stmt = null;

	public static void connection() throws ClassNotFoundException, SQLException {
		if (conn == null) {
			conn = getConn();
			stmt = conn.createStatement();
		}
	}

	public static void executeSql(String sql) throws SQLException, ClassNotFoundException {
		System.out.println(sql);
		connection();
		stmt.executeQuery(sql);
	}

	public static Connection getConn() throws ClassNotFoundException, SQLException {
		Class.forName(driverName);
		Connection conn = DriverManager.getConnection(url, user, password);
		return conn;
	}

}