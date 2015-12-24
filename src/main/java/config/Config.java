package config;

import java.util.ResourceBundle;

public class Config {

	// 换行符
	public static final String NEW_LINE = System.getProperty("line.separator");

	private static final String BUNDLE_NAME = "config.config";

	private static final ResourceBundle RESOURCE_BUNDLE = ResourceBundle.getBundle(BUNDLE_NAME);

	public static String getString(String key) {
		return RESOURCE_BUNDLE.getString(key);
	}

	// REDIS 地址
	public static final String REDIS_ADDRESS = getString("REDIS_ADDRESS");
	// HIVEJDBC
	public static final String HIVE_JDBC_ADDRESS = getString("HIVE_JDBC_ADDRESS");
	// KAFKA BROKER 地址
	public static final String KAFKA_ZOOKEEPER_ADDRESS = getString("KAFKA_ZOOKEEPER_ADDRESS");
	// KAFKA 地址
	public static final String KAFKA_BROKER_ADDRESS = getString("KAFKA_BROKER_ADDRESS");

	// Hadoop 地址
	public static final String NAMENODE_ADDRESS = getString("NAMENODE_ADDRESS");
	// DB 地址
	public static final String MYSQL_ADDRESS = getString("MYSQL_ADDRESS");
	// 数据库用户名
	public static final String MYSQL_USERNAME = getString("MYSQL_USERNAME");
	// 数据库密码
	public static final String MYSQL_PASSWORD = getString("MYSQL_PASSWORD");
	// 分析主要数据库名称
	public static final String MYSQL_DBNAME_BASIC_ANALYSIS = getString("MYSQL_DBNAME_BASIC_ANALYSIS");
	// 逻辑主要数据库名称
	public static final String MYSQL_DBNAME_LOGIC = getString("MYSQL_DBNAME_LOGIC");
	// PID 文件路径
	public static final String PID_FILE_PATH = getString("PID_FILE_PATH");
	// PID 名称
	public static final String PID_FILE_NAME = getString("PID_FILE_NAME");
	// 系统日志
	public static final String SYSTEM_LOG = getString("SYSTEM_LOG");
	// 插入数据分区数量
	public static final int ORGANIZA_INSERTSQL_NUM_PARTITIONS = Integer.parseInt(getString("ORGANIZA_INSERTSQL_NUM_PARTITIONS"));
	// 合并日志REDUCE数量
	public static final int MERGE_LOG_REDUCE_NUM = Integer.parseInt(getString("MERGE_LOG_REDUCE_NUM"));
	// 报表单次分析文件数量
	public static final int ANALYSIS_LOG_NUM = Integer.parseInt(getString("ANALYSIS_LOG_NUM"));
	// 分隔符系列
	public static final String REPORT_KEY_DELIMITED = getString("REPORT_KEY_DELIMITED");
	// 分隔符系列
	public static final String REPORT_VALUE_DELIMITED = getString("REPORT_VALUE_DELIMITED");
	// 分隔符系列
	public static final String LOG_NAME_DELIMITER = getString("LOG_NAME_DELIMITER");
	// 日志分析目录
	public static final String HDFS_ANALYSIS_WORK = getString("HDFS_ANALYSIS_WORK");
	// 日志合并工作目录
	public static final String HDFS_LOG_MERGE_WORK = getString("HDFS_LOG_MERGE_WORK");
	// 日志备份目录
	public static final String HDFS_LOG_BAK = getString("HDFS_LOG_BAK");
	// 从KAFKA到HDFS的临时目录
	public static final String HDFS_LOG_KAFKA_NORMAL_LOG = getString("HDFS_LOG_KAFKA_NORMAL_LOG");
	// 从KAFKA到HDFS的临时目录
	public static final String HDFS_LOGS_KAFKA_ERR_LOG = getString("HDFS_LOGS_KAFKA_ERR_LOG");
	// 把KAFKA日志转移到可分析的目录下
	public static final String HDFS_LOG_NORMAL = getString("HDFS_LOG_NORMAL");
	// 把KAFKA日志转移到可分析的目录下
	public static final String HDFS_LOG_ERR = getString("HDFS_LOG_ERR");
	// 分析日志目录
	public static final String HDFS_ANALYSIS_BATCH_LOG = getString("HDFS_ANALYSIS_BATCH_LOG");
	// HIVE 路径
	public static final String HIVE_COGTU_LOG_TABLE_PATH = getString("HIVE_COGTU_LOG_TABLE_PATH");
	// HIVE 表名
	public static final String HIVE_COGTU_LOG_TABLE_NAME = getString("HIVE_COGTU_LOG_TABLE_NAME");

	public static void main(String[] args) {
		System.out.println(HIVE_JDBC_ADDRESS);
	}
}
