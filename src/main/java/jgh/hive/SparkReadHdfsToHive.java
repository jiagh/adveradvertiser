package jgh.hive;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;

import config.Config;
import jgh.output.SparkHiveOutputFormat;
import jgh.util.Cogtu_log_splitTookit;
import jgh.util.DateUtil;
import jgh.util.HiveUtil;
import jgh.util.JsonUtil;
import jgh.util.PublicUtil;
import jgh.vo.AdLog1_1;
import jgh.vo.CogtuLog1_1;
import jgh.vo.Cogtu_log_split;
import jgh.vo.ReqLog1_1;
import scala.Tuple2;

public class SparkReadHdfsToHive {

    /**
     * 读取hdfs文件，对原始数据进行折分写入hive表
     * 
     * @param args
     */
    @SuppressWarnings("serial")
    public static void readDataInHdfs(String args[]) {
	if (args.length < 3) {
	    System.err.println("Usage: hdfs <time> <jobname> <numThreads> ");
	    System.exit(1);
	}
	StringBuilder inPutBuilder = new StringBuilder();
	inPutBuilder.append(Config.NAMENODE_ADDRESS).append(Config.HDFS_LOG_BAK).append("/date=").append(args[0]);
	String input = inPutBuilder.toString();
	StringBuilder outPutBuilder = new StringBuilder();
	long t = System.currentTimeMillis();
	outPutBuilder.append(Config.NAMENODE_ADDRESS).append(Config.HDFS_LOG_MERGE_WORK).append("/").append(args[0]).append("-").append(t);
	String writePath = outPutBuilder.toString();
	System.out.println("hdfs output path：" + writePath);
	// init spark
	SparkConf sparkConf = new SparkConf().setAppName(args[1]);
	sparkConf.setMaster("local[" + Integer.parseInt(args[2]) + "]");
	JavaSparkContext ctx = new JavaSparkContext(sparkConf);
	JavaRDD<String> rdd = ctx.textFile(input);

	try {
	    PublicUtil.mkdirHDFSFolder();
	    rdd.mapToPair(new PairFunction<String, String, String>() {
		@Override
		public Tuple2<String, String> call(String t) throws Exception {
		    // TODO Auto-generated method stub
		    return new Tuple2<String, String>("", t);
		}
	    }).flatMapToPair(new PairFlatMapFunction<Tuple2<String, String>, String, Cogtu_log_split>() {

		@Override
		public Iterable<Tuple2<String, Cogtu_log_split>> call(Tuple2<String, String> t) throws Exception {
		    CogtuLog1_1 cl = JsonUtil.toBean(t._2(), CogtuLog1_1.class);
		    ArrayList<Tuple2<String, Cogtu_log_split>> re = new ArrayList<Tuple2<String, Cogtu_log_split>>();
		    if (cl instanceof ReqLog1_1) {
			ReqLog1_1 rl = (ReqLog1_1) cl;

			List<AdLog1_1> adlist = rl.getReqs();
			for (AdLog1_1 log : adlist) {
			    re.add(new Tuple2<String, Cogtu_log_split>(DateUtil.getDayFormat(rl.getTimestamp()), Cogtu_log_splitTookit.setCogtu_log_split(rl, log)));
			}
			return re;
		    }
		    return null;
		}
	    }).mapToPair(new PairFunction<Tuple2<String, Cogtu_log_split>, String, String>() {
		@Override
		public Tuple2<String, String> call(Tuple2<String, Cogtu_log_split> t) throws Exception {
		    return new Tuple2<String, String>(t._1(), Cogtu_log_splitTookit.cogtu_log_splitTookitToString(t._2()));
		}
	    }).partitionBy(new Partitioner() {
		int parNum = 24;

		@Override
		public int numPartitions() {
		    return parNum;
		}

		@Override
		public int getPartition(Object arg0) {
		    String str = arg0.toString();
		    String hourStr = str.substring(str.lastIndexOf("-") + 1, str.length());
		    return getHour(hourStr);
		}
	    }).saveAsHadoopFile(writePath, String.class, String.class, SparkHiveOutputFormat.class);
	    // 写入hive
	    FileSystem fs = FileSystem.get(URI.create(Config.NAMENODE_ADDRESS), new Configuration());
	    Path[] listPaths = FileUtil.stat2Paths(fs.listStatus(new Path(Config.HDFS_LOG_MERGE_WORK + "/" + args[0] + "-" + t + "/")));
	    for (int i = 0; i < listPaths.length; i++) {
		System.out.println(listPaths[i]);
		String path = listPaths[i].toString();
		if (!path.endsWith("SUCCESS")) {
		    String hourstr = path.substring(path.length() - 2, path.length());
		    String time = path.substring(path.lastIndexOf("=") + 1, path.length());
		    System.out.println("LOAD DATA  INPATH '" + listPaths[i] + "' OVERWRITE INTO TABLE adnet_da_report.cogtu_log_split" + " PARTITION(date='" + time + "',hour="
			    + getHour(hourstr) + ")");
//		    HiveUtil.executeSql(
//			    "LOAD DATA INPATH '" + listPaths[i] + "' OVERWRITE INTO TABLE jgh.cogtu_log_spilt" + " PARTITION(date='" + time + "',hour=" + getHour(hourstr) + ")");
		}
	    }
	    // fs.delete(new Path(Config.HDFS_LOG_MERGE_WORK + "/" + args[0] +
	    // "-" + t), true);
	} catch (IOException e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
	} catch (Exception e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
	} finally {
	    ctx.stop();
	    ctx.close();
	}

    }

    public static void main(String args[]) {
	//readDataInHdfs(args);
	loadData(args);
    }

    /**
     * 获取原始点击文件中的小时,用于分区
     * 
     * @param hourstr
     * @return
     */
    public static int getHour(String hourstr) {
	if (hourstr.startsWith("0")) {
	    hourstr = hourstr.replace("0", "");
	}
	return Integer.parseInt(hourstr);
    }

    public static ArrayList<String> getInputFilePath(ArrayList<String> timeList) {
	ArrayList<String> inputPath = new ArrayList<String>();
	for (String time : timeList) {
	    StringBuilder inputBuilder = new StringBuilder();
	    inputBuilder.append(Config.NAMENODE_ADDRESS).append(Config.HDFS_LOG_BAK).append("/").append("date=").append(time);
	    inputPath.add(inputBuilder.toString());
	}
	return inputPath;
    }

    public static void loadData(String args[]) {
	// 写入hive
	try {
	    FileSystem fs = FileSystem.get(URI.create(Config.NAMENODE_ADDRESS), new Configuration());
	    Path[] listPaths = FileUtil.stat2Paths(fs.listStatus(new Path("/hive/jgh")));
	    for (int i = 0; i < listPaths.length; i++) {
		System.out.println(listPaths[i]);
		String path = listPaths[i].toString();
		if (!path.endsWith("SUCCESS")) {
		    String hourstr = path.substring(path.length() - 2, path.length());
		    String time = path.substring(path.lastIndexOf("=") + 1, path.length());
		    System.out.println("LOAD DATA  INPATH '" + listPaths[i] + "' OVERWRITE INTO TABLE adnet_da_report.cogtu_log_split" + " PARTITION(date='" + time + "',hour="
			    + getHour(hourstr) + ")");
		    HiveUtil.executeSql("LOAD DATA INPATH '" + listPaths[i] + "'  INTO TABLE adnet_da_report.cogtu_log_split" + " PARTITION(date='2015-12-03',hour="
			    + getHour(hourstr) + ")");
		}
	    }
	} catch (Exception ex) {
	    ex.printStackTrace();
	}

    }

}
