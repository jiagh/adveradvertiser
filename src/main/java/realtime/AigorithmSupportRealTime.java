package realtime;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import config.Config;
import redis.clients.jedis.JedisCluster;
import scala.Tuple2;
import util.DateUtil;
import util.JsonUtil;
import util.PublicUtil;
import vo.log1_2.AdLog1_2;
import vo.log1_2.CogtuLog1_2;
import vo.log1_2.ReqLog1_2;

/**
 * The Class AdvertisIndexTermsToRedis.
 */
public class AigorithmSupportRealTime {

	/**
	 * 读取hdfs文件，按小时统计creative,campaign,slot
	 *
	 * @return the adv pv click
	 */
	@SuppressWarnings("serial")
	public static void getAdvertisPvClick(String args[]) {
		SparkConf sparkConf = new SparkConf().setAppName("AigorithmSupportRealTime");
		final JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(Integer.parseInt(args[0])));
		int numThreads = Integer.parseInt(args[3]);
		Map<String, Integer> topicMap = new HashMap<String, Integer>();
		String[] topics = args[2].split(",");

		for (String topic : topics) {
			topicMap.put(topic, numThreads);
		}
		JavaPairReceiverInputDStream<String, String> logs = KafkaUtils.createStream(jssc, Config.KAFKA_ZOOKEEPER_ADDRESS, args[1], topicMap, StorageLevel.MEMORY_AND_DISK());

		JavaPairDStream<String, TreeMap<String, String>> dstream = logs.flatMapToPair(new PairFlatMapFunction<Tuple2<String, String>, String, ArrayList<Integer>>() {

			@Override
			public Iterable<Tuple2<String, ArrayList<Integer>>> call(Tuple2<String, String> t) throws Exception {
				CogtuLog1_2 cl = JsonUtil.toBean(t._2().toString(), CogtuLog1_2.class);
				ArrayList<Tuple2<String, ArrayList<Integer>>> re = new ArrayList<Tuple2<String, ArrayList<Integer>>>();
				ArrayList<Integer> keycount = new ArrayList<Integer>(Arrays.asList(0, 0));
				if (cl instanceof ReqLog1_2) {
					ReqLog1_2 r1 = JsonUtil.toBean(t._2().toString(), ReqLog1_2.class);
					for (AdLog1_2 log : r1.getReqs()) {
						if (r1.getReqType() == 2 && log.getCampaignType() != 1) {
							keycount.set(0, keycount.get(0) + 1);
						} else if (r1.getReqType() == 3 && log.getCampaignType() != 1) {
							keycount.set(1, keycount.get(1) + 1);
						}
						String dateValue = DateUtil.getTimeStampFormat(cl.getTimestamp(), DateUtil.yyyyMMddHH);
						re.add(new Tuple2<String, ArrayList<Integer>>(dateValue + ":asd-slot-" + r1.getSlotId(), keycount));
						re.add(new Tuple2<String, ArrayList<Integer>>(dateValue + ":asd-campaign-" + log.getCampaignId(), keycount));
						re.add(new Tuple2<String, ArrayList<Integer>>(dateValue + ":asd-creative-" + log.getCreativeId(), keycount));
					}
				}
				return re;
			}
		}).reduceByKey(new Function2<ArrayList<Integer>, ArrayList<Integer>, ArrayList<Integer>>() {
			@Override
			public ArrayList<Integer> call(ArrayList<Integer> v1, ArrayList<Integer> v2) throws Exception {
				ArrayList<Integer> value = new ArrayList<Integer>();
				for (int i = 0; i < v1.size(); i++) {
					value.add(v1.get(i).intValue() + v2.get(i).intValue());
				}
				return value;
			}
		}).mapToPair(new PairFunction<Tuple2<String, ArrayList<Integer>>, String, TreeMap<String, String>>() {

			@Override
			public Tuple2<String, TreeMap<String, String>> call(Tuple2<String, ArrayList<Integer>> t) throws Exception {
				String keyStr[] = t._1().split(":");
				TreeMap<String, String> map = new TreeMap<String, String>();
				map.put(keyStr[0], t._2().get(0) + "," + t._2().get(1));
				return new Tuple2<String, TreeMap<String, String>>(keyStr[1], map);
			}
		}).reduceByKey(new Function2<TreeMap<String, String>, TreeMap<String, String>, TreeMap<String, String>>() {
			@Override
			public TreeMap<String, String> call(TreeMap<String, String> v1, TreeMap<String, String> v2) throws Exception {
				v1.putAll(v2);
				return v1;
			}
		}).repartition(1);
		dstream.foreachRDD(new Function<JavaPairRDD<String, TreeMap<String, String>>, Void>() {
			@Override
			public Void call(JavaPairRDD<String, TreeMap<String, String>> v1) throws Exception {
				v1.foreachPartition(new VoidFunction<Iterator<Tuple2<String, TreeMap<String, String>>>>() {
					@SuppressWarnings("unchecked")
					@Override
					public void call(Iterator<Tuple2<String, TreeMap<String, String>>> t) throws Exception {
						JedisCluster redis = PublicUtil.redisCluster();
						while (t.hasNext()) {
							Tuple2<String, TreeMap<String, String>> tup = t.next();
							TreeMap<String, String> redMap = tup._2();
							if (null != redis.get(tup._1())) {
								redMap = JsonUtil.toBean(redis.get(tup._1()), tup._2().getClass());
							}
							redis.setex(tup._1(), 24 * 3600, JsonUtil.toJson(mapToPlusByPvClick(redMap, tup._2())));
						}

					}
				});
				return null;
			}
		});
		jssc.start();
		jssc.awaitTermination();
	}

	public static TreeMap<String, String> mapToPlusByPvClick(TreeMap<String, String> beforeMap, TreeMap<String, String> addMap) {

		for (String key : addMap.keySet()) {
			if (null == beforeMap.get(key)) {
				beforeMap.put(key, addMap.get(key));
			} else {
				String before[] = beforeMap.get(key).split(",");
				String add[] = addMap.get(key).split(",");
				int pv = Integer.parseInt(before[0]) + Integer.parseInt(add[0]);
				int click = Integer.parseInt(before[1]) + Integer.parseInt(add[1]);
				beforeMap.put(key, pv + "," + click);
			}
		}
		if (beforeMap.size() > 24) {
			beforeMap.remove(beforeMap.firstKey());
		}
		return beforeMap;
	}

	public static void main(String args[]) {

		if (args.length < 4) {
			System.err.println("Usage: AigorithmSupportRealTime <second> <group> <topics> <numThreads>");
			System.exit(1);
		}

		getAdvertisPvClick(args);
	}

}
