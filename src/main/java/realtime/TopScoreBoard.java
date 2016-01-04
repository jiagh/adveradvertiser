package realtime;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
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
import util.PublicUtil;

public class TopScoreBoard extends Thread {

	private int second = 300;
	private int top = 5000;

	public int getSecond() {
		return second;
	}

	public void setSecond(int second) {
		this.second = second;
	}

	public int getTop() {
		return top;
	}

	public void setTop(int top) {
		this.top = top;
	}

	@Override
	public void run() {

		super.run();

		while (true) {
			try {
				System.out.println("清理开始 ");
				JedisCluster redis = PublicUtil.redisCluster();
				redis.ltrim("TopScoreboard", 0, getTop());
				redis.close();
				System.out.println("清理结束");
				Thread.sleep(second * 1000);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public static void main(String[] args) {

		if (args.length < 5) {
			System.err.println("Usage: TopScoreboard <second> <group> <topics> <numThreads> <topNum>");
			System.exit(1);
		}

		TopScoreBoard tb = new TopScoreBoard();
		tb.setTop(Integer.parseInt(args[4]));
		tb.setSecond(Integer.parseInt(args[0]));
		tb.start();

		// 初始化配置
		SparkConf sparkConf = new SparkConf().setAppName("TopScoreBoard");
		// 配置流上下文
		JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(Integer.parseInt(args[0])));

		// 线程数
		int numThreads = Integer.parseInt(args[3]);
		Map<String, Integer> topicMap = new HashMap<String, Integer>();
		String[] topics = args[2].split(",");
		for (String topic : topics) {
			topicMap.put(topic, numThreads);
		}

		// 数据流RDD
		JavaPairReceiverInputDStream<String, String> logs = KafkaUtils.createStream(jssc, Config.KAFKA_ZOOKEEPER_ADDRESS, args[1], topicMap, StorageLevel.MEMORY_AND_DISK());

		JavaPairDStream<String, Integer> step1 = logs.mapToPair(new PairFunction<Tuple2<String, String>, String, Integer>() {

			/**
			 * 拆分 KEY VALUE
			 */
			private static final long serialVersionUID = -8727359511314491256L;

			@Override
			public Tuple2<String, Integer> call(Tuple2<String, String> t) throws Exception {
				return new Tuple2<String, Integer>(t._2(), 1);
			}

		});

		JavaPairDStream<String, Integer> step2 = step1.reduceByKey(new Function2<Integer, Integer, Integer>() {

			/**
			 * 根据 KEY 累加 VALUE
			 */
			private static final long serialVersionUID = -7471480647788848920L;

			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1 + v2;
			}
		});

		JavaPairDStream<Integer, String> step3 = step2.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {

			/**
			 * 交换KEY 和 VALUE
			 */
			private static final long serialVersionUID = -6752876796482988049L;

			@Override
			public Tuple2<Integer, String> call(Tuple2<String, Integer> t) throws Exception {
				return t.swap();
			}
		});

		JavaPairDStream<Integer, String> step4 = step3.transformToPair(new Function<JavaPairRDD<Integer, String>, JavaPairRDD<Integer, String>>() {

			/**
			 * 倒序 KEY
			 */
			private static final long serialVersionUID = 2401174915423941069L;

			@Override
			public JavaPairRDD<Integer, String> call(JavaPairRDD<Integer, String> v1) throws Exception {
				return v1.sortByKey(true);
			}
		});

		step4.foreachRDD(new Function<JavaPairRDD<Integer, String>, Void>() {

			private static final long serialVersionUID = 8263733757864249098L;

			@Override
			public Void call(JavaPairRDD<Integer, String> v1) throws Exception {

				v1.foreachPartition(new VoidFunction<Iterator<Tuple2<Integer, String>>>() {

					/**
					 * 
					 */
					private static final long serialVersionUID = -6953958951742768327L;

					@Override
					public void call(Iterator<Tuple2<Integer, String>> t) throws Exception {

						JedisCluster redis = PublicUtil.redisCluster();
						while (t.hasNext()) {
							Tuple2<Integer, String> t2 = t.next();
							redis.lpush("TopScoreboard", t2._2());
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
