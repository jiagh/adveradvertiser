package spark;

import java.io.IOException;
import java.net.URI;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.storage.StorageLevel;

import config.Config;
import output.MergeHdfsLogSparkOutputFormat;
import scala.Tuple2;
import util.DateUtil;
import util.HiveUtil;
import util.JsonUtil;
import util.MysqlUtil;
import util.PublicUtil;
import util.SystemLogUtil;
import vo.log1_2.CogtuLog1_2;
import vo.log1_2.ReqLog1_2;

public class MergeLog {

	public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException, SQLException {

		if (args.length < 1) {
			System.err.println("Usage: Frequent <date>");
			System.exit(1);
		}

		go(args[0]);
	}

	public static void go(String date) throws IOException, ClassNotFoundException, InterruptedException, SQLException {

		long t = System.currentTimeMillis();

		MysqlUtil db = new MysqlUtil();
		if (db.selectSql("SELECT 1 FROM offline_basic_analysis.merge_log_list WHERE day = '" + date + "'").size() == 0) {
			db.insertSql("INSERT INTO merge_log_list (day,status) VALUES ('" + date + "','N')");
		}

		ArrayList<HashMap<String, Object>> info = db.selectSql("SELECT day FROM offline_basic_analysis.merge_log_list WHERE day='" + date + "' AND status = 'N' ");

		if (info.size() > 0) {

			String year = date.replaceAll("-", "").substring(0, 4);
			String month = date.replaceAll("-", "").substring(4, 6);
			String day = date.replaceAll("-", "").substring(6, 8);

			// 在HDFS上创建一些必要的文件夹
			PublicUtil.mkdirHDFSFolder();

			String inPath = Config.HIVE_COGTU_LOG_TABLE_PATH + "/date=" + year + "-" + month + "-" + day;
			String outPath = Config.HDFS_LOG_MERGE_WORK + "/" + date + "-" + t;

			// 开始合并
			mergeRun(inPath, outPath);

			// 创建HDFS连接
			FileSystem fs = FileSystem.get(URI.create(Config.NAMENODE_ADDRESS), new Configuration());
			fs.rename(new Path(inPath), new Path(Config.HDFS_LOG_BAK + "/"));
			fs.delete(new Path(outPath + "/_SUCCESS"), true);

			// 插入文件到HIVE表
			Path[] listPaths = FileUtil.stat2Paths(fs.listStatus(new Path(outPath)));
			for (int i = 0; i < listPaths.length; i++) {
				System.out.println(listPaths[i]);
				HiveUtil.executeSql("LOAD DATA INPATH '" + listPaths[i] + "'  INTO TABLE " + Config.HIVE_COGTU_LOG_TABLE_NAME + " PARTITION(date='" + year + "-" + month + "-" + day + "')");
			}

			db.insertSql("UPDATE offline_basic_analysis.merge_log_list SET status = 'Y' WHERE day = '" + date + "'");

		} else {
			db.insertSql("UPDATE offline_basic_analysis.merge_log_list SET status = 'ERR' WHERE day = '" + date + "'");
			SystemLogUtil.WriteLog("merge hdfs Log Err", SystemLogUtil.ERROR, "");
		}

	}

	public static void mergeRun(String in, String out) {

		SparkConf sparkConf = new SparkConf().setAppName("MergeLogRun");
		SparkContext sc = new SparkContext(sparkConf);
		JavaSparkContext ctx = new JavaSparkContext(sc);

		// 加载数据
		JavaRDD<String> sourceLog = ctx.textFile(in).persist(StorageLevel.MEMORY_AND_DISK());

		sourceLog.mapToPair(new PairFunction<String, String, String>() {

			/**
			 * Generate File Name
			 */
			private static final long serialVersionUID = -8401090599199317414L;

			@Override
			public Tuple2<String, String> call(String t) throws Exception {
				CogtuLog1_2 cl = JsonUtil.toBean(t, CogtuLog1_2.class);
				if (cl instanceof ReqLog1_2) {
					ReqLog1_2 rl = (ReqLog1_2) cl;
					String date = DateUtil.getTimeStampFormat(rl.getTimestamp(), DateUtil.yyyyMMddHH);
					String day = DateUtil.getTimeStampFormat(rl.getTimestamp(), DateUtil.yyyyMMdd);

					if (rl.getReqType() == 1) {
						return new Tuple2<String, String>(("fetch-" + date), t);
					} else if (rl.getReqType() == 2) {
						return new Tuple2<String, String>(("imp-" + date), t);
					} else if (rl.getReqType() == 3) {
						return new Tuple2<String, String>(("click-" + day), t);
					} else if (rl.getReqType() == -1) {
						return new Tuple2<String, String>(("trouble-" + day), t);
					}
				}
				return null;
			}
		}).partitionBy(new Partitioner() {

			/**
			 * Manually Partitioner
			 */
			private static final long serialVersionUID = 4653690889932658978L;

			@Override
			public int numPartitions() {
				return Config.MERGE_LOG_REDUCE_NUM;
			}

			@Override
			public int getPartition(Object arg0) {

				return Math.abs(arg0.toString().hashCode() % numPartitions());
			}
		}).saveAsHadoopFile(out, String.class, String.class, MergeHdfsLogSparkOutputFormat.class);

	}

}
