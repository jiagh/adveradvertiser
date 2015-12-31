package jgh.util;

import java.io.IOException;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import config.Config;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

public class PublicUtil implements Serializable {

    /**
     * 
     */
    private static final long serialVersionUID = 8809512376815463354L;

    public static FileSystem getFs() throws IOException {
	return FileSystem.get(URI.create(Config.NAMENODE_ADDRESS), new Configuration());

    }

    // REDIS集群创建连接
    public static JedisCluster redisCluster() {
	Set<HostAndPort> jedisClusterNodes = new HashSet<HostAndPort>();
	String ip[] = Config.REDIS_ADDRESS.split(",");
	for (int i = 0; i < ip.length; i++) {
	    String tmp[] = ip[i].split(":");
	    jedisClusterNodes.add(new HostAndPort(tmp[0], Integer.parseInt(tmp[1])));
	}
	JedisCluster jc = new JedisCluster(jedisClusterNodes);
	return jc;
    }

    /**
     * 初始化分析所需要的全部目录
     */
    public static void mkdirHDFSFolder() throws IOException {

	// 创建HDFS连接
	FileSystem fs = getFs();

	if (!fs.exists(new Path(Config.HDFS_LOG_NORMAL)))
	    fs.mkdirs(new Path(Config.HDFS_LOG_NORMAL));

	if (!fs.exists(new Path(Config.HDFS_LOG_KAFKA_NORMAL_LOG)))
	    fs.mkdirs(new Path(Config.HDFS_LOG_KAFKA_NORMAL_LOG));

	if (!fs.exists(new Path(Config.HDFS_LOG_ERR)))
	    fs.mkdirs(new Path(Config.HDFS_LOG_ERR));

	if (!fs.exists(new Path(Config.HDFS_LOGS_KAFKA_ERR_LOG)))
	    fs.mkdirs(new Path(Config.HDFS_LOGS_KAFKA_ERR_LOG));

	if (!fs.exists(new Path(Config.HDFS_LOG_BAK)))
	    fs.mkdirs(new Path(Config.HDFS_LOG_BAK));

	if (!fs.exists(new Path(Config.HDFS_ANALYSIS_BATCH_LOG)))
	    fs.mkdirs(new Path(Config.HDFS_ANALYSIS_BATCH_LOG));

	if (!fs.exists(new Path(Config.HDFS_ANALYSIS_WORK)))
	    fs.mkdirs(new Path(Config.HDFS_ANALYSIS_WORK));

	if (!fs.exists(new Path(Config.HDFS_LOG_MERGE_WORK)))
	    fs.mkdirs(new Path(Config.HDFS_LOG_MERGE_WORK));

    }

    /**
     * 初始化分析所需要的全部目录
     */
    public static JavaSparkContext getJavaSparkContext(String appname) {

	SparkConf sparkConf = new SparkConf().setAppName(appname);
	sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
	sparkConf.set("spark.kryo.registrator", "vo.MyKryoRegistrator");
	JavaSparkContext ctx = new JavaSparkContext(sparkConf);

	return ctx;
    }

    /**
     * 首字母转大写
     */
    public static String toUpperCaseFirstOne(String s) {
	if (Character.isUpperCase(s.charAt(0)))
	    return s;
	else
	    return (new StringBuilder()).append(Character.toUpperCase(s.charAt(0))).append(s.substring(1)).toString();
    }

    /**
     * 编码
     */
    public static String encodeUTF8(String str) throws UnsupportedEncodingException {
	return URLEncoder.encode(str, "UTF-8");
    }

    /**
     * 解码
     */
    public static String decodeUTF8(String str) throws UnsupportedEncodingException {
	return URLDecoder.decode(str, "UTF-8");
    }
}
