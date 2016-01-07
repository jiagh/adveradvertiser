package config;

import java.util.ResourceBundle;

public class Config {

    private static final ResourceBundle RESOURCE_BUNDLE = ResourceBundle.getBundle("config.config");

    public static String getString(String key) {
	return RESOURCE_BUNDLE.getString(key);
    }

    // Hadoop 地址
    public static final String NAMENODE_ADDRESS = getString("NAMENODE_ADDRESS");
    // 日志合并工作目录
    public static final String HDFS_LOG_MERGE_WORK = getString("HDFS_LOG_MERGE_WORK");
    // 日志备份目录
    public static final String HDFS_LOG_BAK = getString("HDFS_LOG_BAK");
}
