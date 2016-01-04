package realtime;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeSet;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import config.Config;
import redis.clients.jedis.JedisCluster;
import scala.Tuple2;
import spark.process.FilterLog;
import util.GzipUtil;
import util.JsonUtil;
import util.PublicUtil;
import vo.log1_2.ReqLog1_2;

public class Frequency {

	public static void main(String[] args) throws IOException {

		if (args.length < 4) {
			System.err.println("Usage: Frequent <second> <group> <topics> <numThreads>");
			System.exit(1);
		}

		SparkConf sparkConf = new SparkConf().setAppName("Frequency");
		final JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(Integer.parseInt(args[0])));

		/**
		 * Kafka Thread Num
		 */
		int numThreads = Integer.parseInt(args[3]);
		Map<String, Integer> topicMap = new HashMap<String, Integer>();
		String[] topics = args[2].split(",");
		for (String topic : topics) {
			topicMap.put(topic, numThreads);
		}

		JavaPairReceiverInputDStream<String, String> logs = KafkaUtils.createStream(jssc, Config.KAFKA_ZOOKEEPER_ADDRESS, args[1], topicMap, StorageLevel.MEMORY_AND_DISK());

		logs.map(new Function<Tuple2<String, String>, String>() {

			/**
			 * Data Collection
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public String call(Tuple2<String, String> v1) throws Exception {
				return v1._2();
			}
		}).transform(new Function<JavaRDD<String>, JavaRDD<String>>() {

			/**
			 * Filter Log
			 */
			private static final long serialVersionUID = -7713069507427491686L;

			@Override
			public JavaRDD<String> call(JavaRDD<String> v1) throws Exception {
				FilterLog fl = new FilterLog();
				JavaRDD<String> log = fl.execute(v1);
				return log;
			}
		}).flatMapToPair(new PairFlatMapFunction<String, String, Integer>() {

			/**
			 * Generate Key Value
			 */
			private static final long serialVersionUID = 5834775184774779790L;

			@Override
			public Iterable<Tuple2<String, Integer>> call(String t) throws Exception {
				ArrayList<Tuple2<String, Integer>> re = new ArrayList<Tuple2<String, Integer>>();
				try {
					if (t.indexOf("\"source\":\"req\"") != -1) {
						ReqLog1_2 rl = JsonUtil.toBean(t, ReqLog1_2.class);
						if (rl.getReqType() == ReqLog1_2.REQ_TYPE_AD_IMP) {
							for (int i = 0; i < rl.getReqs().size(); i++) {
								re.add(new Tuple2<String, Integer>(rl.getUid() + ":" + rl.getReqs().get(i).getCampaignId(), Integer.parseInt((rl.getTimestamp() + "").substring(0, 10))));
							}
						}
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
				return re;
			}
		}).groupByKey().foreachRDD(new Function<JavaPairRDD<String, Iterable<Integer>>, Void>() {

			/**
			 * Foreach Partition
			 */
			private static final long serialVersionUID = -8132518613595017755L;

			@Override
			public Void call(JavaPairRDD<String, Iterable<Integer>> v1) throws Exception {

				v1.foreachPartition(new VoidFunction<Iterator<Tuple2<String, Iterable<Integer>>>>() {

					/**
					 * Compare Timeout Than Insert Result To Redis
					 */
					private static final long serialVersionUID = 1049536475333503595L;

					public TreeSet<Integer> compareTimeStamp(TreeSet<Integer> old, int nowtime, int secondTimeout) {

						TreeSet<Integer> reNew = (TreeSet<Integer>) old.clone();
						Iterator<Integer> it = old.iterator();

						if (old.size() > 0) {
							while (it.hasNext()) {
								int tmp = it.next();
								if ((nowtime - tmp) > secondTimeout) {
									reNew.remove(tmp);
								} else {
									break;
								}
							}
						}

						reNew.add(nowtime);
						return reNew;
					}

					@Override
					public void call(Iterator<Tuple2<String, Iterable<Integer>>> t) throws Exception {

						JedisCluster redis = PublicUtil.redisCluster();
						while (t.hasNext()) {

							// 取得一个用户的展示数据
							Tuple2<String, Iterable<Integer>> t2 = t.next();
							Iterator<Integer> perUidIt = t2._2().iterator();
							TreeSet<Integer> histroyDay = new TreeSet<Integer>();
							TreeSet<Integer> histroyHour = new TreeSet<Integer>();
							String day = redis.get("frequency:time:day:" + t2._1());
							String hour = redis.get("frequency:time:hour:" + t2._1());

							try {
								if (day != null) {
									System.out.println(day);
									histroyDay = JsonUtil.toBean(GzipUtil.gunzip(day), TreeSet.class);
								}
								if (hour != null) {
									histroyHour = JsonUtil.toBean(GzipUtil.gunzip(hour), TreeSet.class);
								}
							} catch (Exception e) {

							}

							// Currently Imp TimeStamp
							TreeSet<Integer> nowTime = new TreeSet<Integer>();
							while (perUidIt.hasNext()) {
								nowTime.add(perUidIt.next());
							}

							// Start Compare TimeOut
							Iterator<Integer> nowTimeIt = nowTime.iterator();
							while (nowTimeIt.hasNext()) {
								int nt = nowTimeIt.next();
								histroyDay = (compareTimeStamp(histroyDay, nt, 3600 * 24));
								histroyHour = (compareTimeStamp(histroyHour, nt, 3600));
							}

							// Insert Day Redis Imp Time Gzip Compression
							redis.setex("frequency:time:day:" + t2._1(), 3600 * 24, GzipUtil.gzip(JsonUtil.toJson(histroyDay)));
							// Insert Hour Redis Imp Time Gzip Compression
							redis.setex("frequency:time:hour:" + t2._1(), 3600, GzipUtil.gzip(JsonUtil.toJson(histroyHour)));

							// Insert Day Frequency Value
							redis.setex("frequency:day:" + t2._1(), 3600 * 24, histroyDay.size() + "");
							// Insert Hour Frequency Value
							redis.setex("frequency:hour:" + t2._1(), 3600, histroyHour.size() + "");

						}
						redis.close();
					}
				});
				return null;
			}
		});

		jssc.start();
		jssc.awaitTermination();

	}
}
