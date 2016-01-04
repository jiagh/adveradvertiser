//package mapreduce;
//
//import java.io.IOException;
//import java.net.URI;
//import java.sql.SQLException;
//import java.util.ArrayList;
//import java.util.HashMap;
//import java.util.Iterator;
//
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.fs.FileSystem;
//import org.apache.hadoop.fs.FileUtil;
//import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapreduce.Job;
//import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
//import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//
//import config.Config;
//import output.MergeHdfsLogOutputFormat;
//import util.DateUtil;
//import util.HiveUtil;
//import util.JsonUtil;
//import util.MysqlUtil;
//import util.PublicUtil;
//import util.SystemLogUtil;
//import vo.log1_1.CogtuLog1_1;
//import vo.log1_1.ReqLog1_1;
//
//public class MergeHdfsLogRun {
//
//	public static class MergeHdfsLogMapper extends Mapper<Object, Text, Text, Text> {
//
//		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
//			try {
//				CogtuLog1_1 cl = JsonUtil.toBean(value.toString(), CogtuLog1_1.class);
//				if (cl instanceof ReqLog1_1) {
//					ReqLog1_1 rl = (ReqLog1_1) cl;
//					String date = DateUtil.getTimeStampFormat(rl.getTimestamp(), DateUtil.yyyyMMddHH);
//					String day = DateUtil.getTimeStampFormat(rl.getTimestamp(), DateUtil.yyyyMMdd);
//
//					if (rl.getReqType() == 1) {
//						context.write(new Text("fetch-" + date), value);
//					} else if (rl.getReqType() == 2) {
//						context.write(new Text("imp-" + date), value);
//					} else if (rl.getReqType() == 3) {
//						context.write(new Text("click-" + day), value);
//					} else if (rl.getReqType() == -1) {
//						context.write(new Text("trouble-" + day), value);
//					}
//				}
//			} catch (Exception e) {
//				e.printStackTrace();
//			}
//		}
//	}
//
//	public static class MergeHdfsLogReducer extends Reducer<Text, Text, Text, Text> {
//
//		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
//			Iterator<Text> it = values.iterator();
//			while (it.hasNext()) {
//				context.write(key, it.next());
//			}
//		}
//	}
//
//	public void go(String date) throws IOException, ClassNotFoundException, InterruptedException, SQLException {
//
//		long t = System.currentTimeMillis();
//
//		MysqlUtil db = new MysqlUtil();
//		if (db.selectSql("SELECT 1 FROM offline_basic_analysis.merge_log_list WHERE day = '" + date + "'").size() == 0) {
//			db.insertSql("INSERT INTO merge_log_list (day,status) VALUES ('" + date + "','N')");
//		}
//
//		ArrayList<HashMap<String, Object>> info = db.selectSql("SELECT day FROM offline_basic_analysis.merge_log_list WHERE day='" + date + "' AND status = 'N' ");
//
//		if (info.size() > 0) {
//
//			String year = date.replaceAll("-", "").substring(0, 4);
//			String month = date.replaceAll("-", "").substring(4, 6);
//			String day = date.replaceAll("-", "").substring(6, 8);
//
//			// 在HDFS上创建一些必要的文件夹
//			PublicUtil.mkdirHDFSFolder();
//
//			Configuration conf = new Configuration();
//
//			Job job = Job.getInstance(conf, "MergeHdfsLogRun");
//			job.setJarByClass(MergeHdfsLogRun.class);
//			job.setMapperClass(MergeHdfsLogMapper.class);
//			job.setReducerClass(MergeHdfsLogReducer.class);
//			job.setOutputKeyClass(Text.class);
//			job.setOutputValueClass(Text.class);
//			job.setNumReduceTasks(Config.MERGE_LOG_REDUCE_NUM);
//			job.setOutputFormatClass(MergeHdfsLogOutputFormat.class);
//			FileInputFormat.addInputPath(job, new Path(Config.HIVE_COGTU_LOG_TABLE_PATH + "/date=" + year + "-" + month + "-" + day));
//			FileOutputFormat.setOutputPath(job, new Path(Config.HDFS_LOG_MERGE_WORK + "/" + date + "-" + t));
//
//			if (job.waitForCompletion(true)) {
//
//				// 创建HDFS连接
//				FileSystem fs = FileSystem.get(URI.create(Config.NAMENODE_ADDRESS), new Configuration());
//				//fs.delete(new Path(Config.HDFS_LOG_BAK + "/" + year + "/" + month + "/" + day), true);
//				fs.delete(new Path(Config.HDFS_LOG_MERGE_WORK + "/" + date + "-" + t + "/_SUCCESS"), true);
//
//				// 插入文件到HIVE表
//				Path[] listPaths = FileUtil.stat2Paths(fs.listStatus(new Path(Config.HDFS_LOG_MERGE_WORK + "/" + date + "-" + t + "/")));
//				for (int i = 0; i < listPaths.length; i++) {
//					System.out.println(listPaths[i]);
//					HiveUtil.executeSql("LOAD DATA INPATH '" + listPaths[i] + "'  INTO TABLE " + Config.HIVE_COGTU_LOG_TABLE_NAME + " PARTITION(date='" + year + "-" + month + "-" + day + "')");
//				}
//
//				db.insertSql("UPDATE offline_basic_analysis.merge_log_list SET status = 'Y' WHERE day = '" + date + "'");
//
//			} else {
//				db.insertSql("UPDATE offline_basic_analysis.merge_log_list SET status = 'ERR' WHERE day = '" + date + "'");
//				SystemLogUtil.WriteLog("merge hdfs Log Err", SystemLogUtil.ERROR, "");
//			}
//
//		}
//
//	}
//
//	public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException, SQLException {
//		new MergeHdfsLogRun().go(args[0]);
//	}
//
//}
