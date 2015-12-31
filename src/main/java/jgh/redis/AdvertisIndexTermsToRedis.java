package jgh.redis;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import config.Config;
import jgh.util.DateUtil;
import jgh.util.JsonUtil;
import jgh.util.PublicUtil;
import jgh.vo.AdLog1_1;
import jgh.vo.CogtuLog1_1;
import jgh.vo.ReqLog1_1;
import redis.clients.jedis.JedisCluster;
import scala.Tuple2;


// TODO: Auto-generated Javadoc
/**
 * The Class AdvertisIndexTermsToRedis.
 */
public class AdvertisIndexTermsToRedis {

    /**
     * Gets the adv pv click.
     *
     * @return the adv pv click
     */
    @SuppressWarnings("serial")
    public static void getAdvertisPvClick(String args[]) {
	SparkConf sparkConf = new SparkConf().setAppName("SummaryByHdfsToPvClick");
	sparkConf.setMaster("local[8]");
	JavaSparkContext ctx = new JavaSparkContext(sparkConf);
	String inputFile = Config.NAMENODE_ADDRESS + Config.HDFS_LOG_BAK+"/date="+args[0];
	JavaRDD<String> rdd = ctx.textFile(inputFile);
	rdd.flatMapToPair(new PairFlatMapFunction<String, String, ArrayList<Integer>>() {

	    @Override
	    public Iterable<Tuple2<String, ArrayList<Integer>>> call(String t) throws Exception {
		CogtuLog1_1 cl = JsonUtil.toBean(t.toString(), CogtuLog1_1.class);
		ArrayList<Tuple2<String, ArrayList<Integer>>> re = new ArrayList<Tuple2<String, ArrayList<Integer>>>();
		ArrayList<Integer> keycount = new ArrayList<Integer>(Arrays.asList(0, 0));
		if (cl instanceof ReqLog1_1) {
		    ReqLog1_1 r1 = JsonUtil.toBean(t, ReqLog1_1.class);
		    for (AdLog1_1 log : r1.getReqs()) {
			if (r1.getReqType() == 2) {
			    keycount.set(0, keycount.get(0) + 1);
			} else if (r1.getReqType() == 3) {
			    keycount.set(1, keycount.get(1) + 1);
			}
			String dateValue = DateUtil.getDayHour(cl.getTimestamp());
			re.add(new Tuple2<String, ArrayList<Integer>>(dateValue + ":slot-" + r1.getSlotId(), keycount));
			re.add(new Tuple2<String, ArrayList<Integer>>(dateValue + ":campaign-" + log.getCampaignId(), keycount));
			re.add(new Tuple2<String, ArrayList<Integer>>(dateValue + ":creative-" + log.getCreativeId(), keycount));
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
	}).mapToPair(new PairFunction<Tuple2<String, ArrayList<Integer>>, String, HashMap<String, ArrayList<Integer>>>() {

	    @Override
	    public Tuple2<String, HashMap<String, ArrayList<Integer>>> call(Tuple2<String, ArrayList<Integer>> t) throws Exception {
		String keyStr[] = t._1().split(":");
		HashMap<String, ArrayList<Integer>> map = new HashMap<String, ArrayList<Integer>>();
		map.put(keyStr[0], t._2());
		return new Tuple2<String, HashMap<String, ArrayList<Integer>>>(keyStr[1], map);
	    }
	}).repartition(1).reduceByKey(new Function2<HashMap<String, ArrayList<Integer>>, HashMap<String, ArrayList<Integer>>, HashMap<String, ArrayList<Integer>>>() {
	    @Override
	    public HashMap<String, ArrayList<Integer>> call(HashMap<String, ArrayList<Integer>> v1, HashMap<String, ArrayList<Integer>> v2) throws Exception {
		v1.putAll(v2);
		return v1;
	    }
	}).foreachPartition(new VoidFunction<Iterator<Tuple2<String, HashMap<String, ArrayList<Integer>>>>>() {
	    @Override
	    public void call(Iterator<Tuple2<String, HashMap<String, ArrayList<Integer>>>> t) throws Exception {
		JedisCluster redis = PublicUtil.redisCluster();
		while (t.hasNext()) {
		    Tuple2<String, HashMap<String, ArrayList<Integer>>> tup = t.next();
		    System.out.format("%s detail:%s %n", tup._1(), JsonUtil.toJson(tup._2()));
		    redis.set(tup._1(), JsonUtil.toJson(tup._2()));
		}
	    }
	});

	ctx.stop();
	ctx.close();
    }

    public static void main(String args[]) {
	getAdvertisPvClick(args);
//	JedisCluster redis = PublicUtil.redisCluster();
//	System.out.println("===:"+redis.get("creative-9"));
    }

}
