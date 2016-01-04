package realtime;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.spark.streaming.scheduler.StreamingListener;
import org.apache.spark.streaming.scheduler.StreamingListenerBatchCompleted;
import org.apache.spark.streaming.scheduler.StreamingListenerBatchStarted;
import org.apache.spark.streaming.scheduler.StreamingListenerBatchSubmitted;
import org.apache.spark.streaming.scheduler.StreamingListenerReceiverError;
import org.apache.spark.streaming.scheduler.StreamingListenerReceiverStarted;
import org.apache.spark.streaming.scheduler.StreamingListenerReceiverStopped;
import output.KafkaDataToHdfsOutputFormat;
import scala.Tuple2;
import util.DateUtil;
import util.JsonUtil;
import util.MysqlUtil;
import util.PublicUtil;
import vo.log1_2.AdLog1_2;
import vo.log1_2.CogtuLog1_2;
import vo.log1_2.ReqLog1_2;
import config.Config;

public class KafkaDataToHdfs {

	public static void main(String[] args) throws IOException {

		if (args.length < 5) {
			System.err.println("Usage: KafkaDataToHdfs <second> <group> <topics> <numThreads> <numPartitions>");
			System.exit(1);
		}

		PublicUtil.mkdirHDFSFolder();

		SparkConf sparkConf = new SparkConf().setAppName("KafkaDataToHdfs");
		final JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(Integer.parseInt(args[0])));

		// 监控何时停止
		jssc.addStreamingListener(new StreamingListener() {

			@Override
			public void onReceiverStopped(StreamingListenerReceiverStopped arg0) {
			}

			@Override
			public void onReceiverStarted(StreamingListenerReceiverStarted arg0) {
			}

			@Override
			public void onReceiverError(StreamingListenerReceiverError arg0) {
			}

			@Override
			public void onBatchSubmitted(StreamingListenerBatchSubmitted arg0) {
			}

			@Override
			public void onBatchStarted(StreamingListenerBatchStarted arg0) {
			}

			@Override
			public void onBatchCompleted(StreamingListenerBatchCompleted arg0) {

				MysqlUtil mt = new MysqlUtil();
				if (mt.selectSql("SELECT 1 FROM streaming WHERE name = 'kafka_to_hdfs' AND status = 'stop'").size() > 0) {
					jssc.stop(true, true);
				}
				mt.close();
			}
		});

		int numThreads = Integer.parseInt(args[3]);
		Map<String, Integer> topicMap = new HashMap<String, Integer>();
		String[] topics = args[2].split(",");
		for (String topic : topics) {
			topicMap.put(topic, numThreads);
		}

		JavaPairReceiverInputDStream<String, String> logs = KafkaUtils.createStream(jssc, Config.KAFKA_ZOOKEEPER_ADDRESS, args[1], topicMap, StorageLevel.MEMORY_AND_DISK());

		// 广告请求日志
		JavaPairDStream<String, String> req = logs.filter(new Function<Tuple2<String, String>, Boolean>() {

			private static final long serialVersionUID = -514917229683850219L;

			@Override
			public Boolean call(Tuple2<String, String> v1) throws Exception {
				if (v1._2().indexOf("\"source\":\"req\"") != -1)
					return true;
				else
					return false;
			}
		});

		JavaPairDStream<String, String> reqAllLog = req.mapToPair(new PairFunction<Tuple2<String, String>, String, String>() {

			/**
			 * 错误日志
			 */
			private static final long serialVersionUID = 8338999061807766471L;

			@Override
			public Tuple2<String, String> call(Tuple2<String, String> t) throws Exception {

				try {

					if (t._2().indexOf("\"source\":\"req\"") != -1) {

						if ((t._2().indexOf("\"reqType\":1,") != -1)) {
							CogtuLog1_2 bl = JsonUtil.toBean(t._2(), CogtuLog1_2.class);
							return new Tuple2<String, String>("fetch-" + DateUtil.getTimeStampFormat(bl.getTimestamp(), DateUtil.yyyyMMddHH), t._2());
						} else if ((t._2().indexOf("\"reqType\":2,") != -1)) {
							CogtuLog1_2 bl = JsonUtil.toBean(t._2(), CogtuLog1_2.class);
							return new Tuple2<String, String>("imp-" + DateUtil.getTimeStampFormat(bl.getTimestamp(), DateUtil.yyyyMMddHH), t._2());
						} else if ((t._2().indexOf("\"reqType\":3,") != -1)) {
							CogtuLog1_2 bl = JsonUtil.toBean(t._2(), CogtuLog1_2.class);
							return new Tuple2<String, String>("click-" + DateUtil.getTimeStampFormat(bl.getTimestamp(), DateUtil.yyyyMMddHH), t._2());
						} else if ((t._2().indexOf("\"reqType\":4,") != -1)) {
							CogtuLog1_2 bl = JsonUtil.toBean(t._2(), CogtuLog1_2.class);
							return new Tuple2<String, String>("template-" + DateUtil.getTimeStampFormat(bl.getTimestamp(), DateUtil.yyyyMMddHH), t._2());
						} else if (t._2().indexOf("\"reqType\":-") != -1) {
							CogtuLog1_2 bl = JsonUtil.toBean(t._2(), CogtuLog1_2.class);
							return new Tuple2<String, String>("trouble-" + DateUtil.getTimeStampFormat(bl.getTimestamp(), DateUtil.yyyyMMddHH), t._2());
						} else {
							return new Tuple2<String, String>("err-" + DateUtil.getNowDate(DateUtil.yyyyMMddHH), t._2());
						}
					}

				} catch (Exception e) {
					return new Tuple2<String, String>("err-" + DateUtil.getNowDate(DateUtil.yyyyMMddHH), t._2());
				}
				return new Tuple2<String, String>("err-" + DateUtil.getNowDate(DateUtil.yyyyMMddHH), t._2());
			}

		});

		JavaPairDStream<String, String> reqErrLog = reqAllLog.filter(new Function<Tuple2<String, String>, Boolean>() {

			/**
			 * 错误日志
			 */
			private static final long serialVersionUID = -7639058452209758925L;

			@Override
			public Boolean call(Tuple2<String, String> v1) throws Exception {

				if (v1._1().indexOf("err-") != -1) {
					return true;
				} else {
					return false;
				}
			}
		});

		reqErrLog.repartition(Integer.parseInt(args[4])).saveAsHadoopFiles(Config.HDFS_LOGS_KAFKA_ERR_LOG + "/" + "log", "", String.class, String.class, KafkaDataToHdfsOutputFormat.class);

		JavaPairDStream<String, String> reqNormalLog = reqAllLog.filter(new Function<Tuple2<String, String>, Boolean>() {

			/**
			 * 正确的日志
			 */
			private static final long serialVersionUID = -7639058452209758925L;

			@Override
			public Boolean call(Tuple2<String, String> v1) throws Exception {

				if (v1._1().indexOf("err-") == -1) {
					return true;
				} else {
					return false;
				}
			}
		});

		reqNormalLog.repartition(Integer.parseInt(args[4])).saveAsHadoopFiles(Config.HDFS_LOG_KAFKA_NORMAL_LOG + "/" + "log", "", String.class, String.class, KafkaDataToHdfsOutputFormat.class);

		JavaPairDStream<String, String> hiveFormat = reqNormalLog.flatMapToPair(new PairFlatMapFunction<Tuple2<String, String>, String, String>() {

			/**
			 * HIVE分隔符格式输出
			 */
			private static final long serialVersionUID = -3154156985724095441L;

			@Override
			public Iterable<Tuple2<String, String>> call(Tuple2<String, String> t) throws Exception {

				ArrayList<Tuple2<String, String>> re = new ArrayList<Tuple2<String, String>>();

				ReqLog1_2 bl = JsonUtil.toBean(t._2(), ReqLog1_2.class);

				StringBuffer sb = new StringBuffer();

				String delimit = Config.HIVE_TABLE_DELIMITER;

				sb.append(bl.getIp() + delimit);
				sb.append(bl.getTimestamp() + delimit);
				sb.append(bl.getSlotId() + delimit);
				sb.append(bl.getSiteId() + delimit);
				sb.append(bl.getPublisherId() + delimit);
				sb.append(bl.getRef() + delimit);
				sb.append(bl.getCountry() + delimit);
				sb.append(bl.getProvince() + delimit);
				sb.append(bl.getCity() + delimit);
				sb.append(bl.getAddrCode() + delimit);
				sb.append(bl.getBrowser() + delimit);
				sb.append(bl.getOs() + delimit);
				sb.append(bl.getUid() + delimit);
				sb.append(bl.getRunTime() + delimit);
				sb.append(bl.getvBalanceCostPrecent() + delimit);
				sb.append(bl.getV() + delimit);
				sb.append(bl.getSessionId() + delimit);
				sb.append(bl.getSource() + delimit);
				sb.append(bl.getAgent() + delimit);
				sb.append(bl.getPageUri() + delimit);

				// FETCH
				re.add(new Tuple2<String, String>(t._1(), sb.toString() + "0" + delimit));

				sb.append(bl.getReqType() + delimit);

				for (int i = 0; i < bl.getReqs().size(); i++) {

					AdLog1_2 al = bl.getReqs().get(i);

					sb.append(al.getImpId() + delimit);
					sb.append(al.getImpUrl() + delimit);
					sb.append(al.getAdvertisersId() + delimit);
					sb.append(al.getProjectId() + delimit);
					sb.append(al.getCampaignId() + delimit);
					sb.append(al.getCreativeId() + delimit);
					sb.append(al.getTemplateId() + delimit);
					sb.append(al.getPrice() + delimit);
					sb.append(al.getPriceType() + delimit);
					sb.append(al.getFeatures() + delimit);
					sb.append(al.getTags() + delimit);
					sb.append(al.getCampaignType() + delimit);

					re.add(new Tuple2<String, String>(t._1(), sb.toString()));
				}
				return re;
			}

		});

		hiveFormat.repartition(Integer.parseInt(args[4])).saveAsHadoopFiles(Config.HDFS_LOG_KAFKA_NORMAL_HIVEFORMAT + "/" + "log", "", String.class, String.class, KafkaDataToHdfsOutputFormat.class);

		jssc.start();
		jssc.awaitTermination();

	}
}
