import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import config.Config;
import scala.Tuple2;
import util.DateUtil;
import util.JsonUtil;
import vo.log1_2.ReqLog1_2;

public class HaoJaTest {

	/**
	 * Format date time interval of 10 minutes
	 */
	public static String minuteInterval10(long time) {

		String date = DateUtil.getTimeStampFormat(time, DateUtil.yyyyMMddHHmm);
		String reDate = date.substring(0, 10);
		int mm = Integer.parseInt(date.substring(10, 12));
		if (mm < 10) {
			reDate += "00";
		} else if (mm < 20) {
			reDate += "10";
		} else if (mm < 30) {
			reDate += "20";
		} else if (mm < 40) {
			reDate += "30";
		} else if (mm < 50) {
			reDate += "40";
		} else if (mm < 60) {
			reDate += "50";
		}    
		return reDate;
	}

	public static void main(String[] args) {
		SparkConf sparkConf = new SparkConf().setAppName("HaoJaTest");
		sparkConf.setMaster("local");
		SparkContext sc = new SparkContext(sparkConf);
		JavaSparkContext ctx = new JavaSparkContext(sc);

		// JavaRDD<String> sourceLog =
		// ctx.textFile("/hive/warehouse/adnet_da_report.db/cogtu_log/date=2015-12-03/").cache();
		JavaRDD<String> sourceLog = ctx.textFile("D:\\A\\S");

		/**
		 * url frist appear datetime
		 */
		JavaPairRDD<String, Long> step1 = sourceLog.mapToPair(new PairFunction<String, String, Long>() {

			/**
			 * Generate key = url,value = timestamp
			 */
			private static final long serialVersionUID = 2674873601932643517L;

			@Override
			public Tuple2<String, Long> call(String t) throws Exception {

				ReqLog1_2 cl = JsonUtil.toBean(t, ReqLog1_2.class);
				String urlSplitParameter = cl.getPageUri().split("\\?")[0];
				return new Tuple2<String, Long>(urlSplitParameter, cl.getTimestamp());
			}
		}).reduceByKey(new Function2<Long, Long, Long>() {

			/**
			 * Compale frist datetime
			 */
			private static final long serialVersionUID = 5441010091363165906L;

			@Override
			public Long call(Long v1, Long v2) throws Exception {
				if (v1 > v2) {
					return v2;
				} else {
					return v1;
				}
			}
		});

		/**
		 * Reduce url minuteInterval10 pv and cache the result rdd to compute
		 * step3
		 */
		JavaPairRDD<String, String> step2_1 = sourceLog.mapToPair(new PairFunction<String, String, Integer>() {

			/**
			 * Generate key = url + minuteInterval10,value = 1
			 */
			private static final long serialVersionUID = 2674873601932643517L;

			@Override
			public Tuple2<String, Integer> call(String t) throws Exception {
				ReqLog1_2 cl = JsonUtil.toBean(t, ReqLog1_2.class);

				if (cl.getReqType() == 1) {
					String urlSplitParameter = cl.getPageUri().split("\\?")[0];
					// key = url + timestamp
					String urlAndMinute = urlSplitParameter + Config.REPORT_KEY_DELIMITED + minuteInterval10(cl.getTimestamp());
					return new Tuple2<String, Integer>(urlAndMinute, 1);
				}

				return null;

			}
		}).filter(new Function<Tuple2<String, Integer>, Boolean>() {

			/**
			 * Filter Null
			 */
			private static final long serialVersionUID = -4507983100027431886L;

			@Override
			public Boolean call(Tuple2<String, Integer> v1) throws Exception {
				if (v1 != null)
					return true;
				else
					return false;
			}
		}).reduceByKey(new Function2<Integer, Integer, Integer>() {

			/**
			 * Reduce pv
			 */
			private static final long serialVersionUID = 8626347944127921914L;

			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1 + v2;
			}
		}).mapToPair(new PairFunction<Tuple2<String, Integer>, String, String>() {

			/**
			 * Generate key = url,value = minuteInterval10 + pv
			 */
			private static final long serialVersionUID = 7268422981570490827L;

			@Override
			public Tuple2<String, String> call(Tuple2<String, Integer> t) throws Exception {
				String tmp[] = t._1().split(Config.REPORT_KEY_DELIMITED);
				return new Tuple2<String, String>(tmp[0], tmp[1] + Config.REPORT_KEY_DELIMITED + t._2());
			} 
		}).cache();

		/**
		 * Compale each url minuteInterval10 get max pv and cache the result rdd
		 * to compute step3
		 */
		JavaPairRDD<String, String> step2_2 = step2_1.reduceByKey(new Function2<String, String, String>() {

			/**
			 * Compale max pv
			 */
			private static final long serialVersionUID = 1036259628632881110L;

			@Override
			public String call(String v1, String v2) throws Exception {
				String tmp1[] = v1.split(Config.REPORT_KEY_DELIMITED);
				String tmp2[] = v2.split(Config.REPORT_KEY_DELIMITED);
				if (Integer.parseInt(tmp1[1]) > Integer.parseInt(tmp2[1])) {

					if (tmp1.length == 2) {
						v1 += Config.REPORT_KEY_DELIMITED + "summit";
					}
					return v1;
				} else {
					if (tmp2.length == 2) {
						v2 += Config.REPORT_KEY_DELIMITED + "summit";
					}
					return v2;
				}
			}
		}).cache();

		/**
		 * Use step2_1 join step2_2 compute step3
		 */
		JavaPairRDD<String, String> step3 = step2_1.union(step2_2).groupByKey().mapToPair(new PairFunction<Tuple2<String, Iterable<String>>, String, String>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 3402845464523878227L;

			@Override
			public Tuple2<String, String> call(Tuple2<String, Iterable<String>> t) throws Exception {
				// TODO Auto-generated method stub

				long summitTime = 0;
				int summitPv = 0;
				Iterator<String> it = t._2.iterator();
				ArrayList<String> list = new ArrayList<String>();

				while (it.hasNext()) {
					String tmp = it.next();
					if (tmp.indexOf("summit") != -1) {
						summitTime = Long.parseLong(tmp.split(Config.REPORT_KEY_DELIMITED)[0]);
						summitPv = Integer.parseInt(tmp.split(Config.REPORT_KEY_DELIMITED)[1]);
					} else {
						list.add(tmp);
					}
				}

				Collections.sort(list, new Comparator<String>() {
					@Override
					public int compare(String o1, String o2) {
						String tmp1[] = o1.toString().split(Config.REPORT_KEY_DELIMITED);
						String tmp2[] = o2.toString().split(Config.REPORT_KEY_DELIMITED);
						return Long.parseLong(tmp1[0]) > Long.parseLong(tmp2[0]) ? 1 : -1;
					}
				});

				for (int i = 0; i < list.size(); i++) {

					String tmp[] = list.get(i).split(Config.REPORT_KEY_DELIMITED);
					if (summitTime < Long.parseLong(tmp[0]) && Integer.parseInt(tmp[1]) < 5) {
						return new Tuple2<String, String>(t._1(), summitTime + "");
					}
				}

				return new Tuple2<String, String>(t._1(), "N");

			}
		});

		step1.join(step2_2).join(step3).map(new Function<Tuple2<String, Tuple2<Tuple2<Long, String>, String>>, String>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 2128462266274959524L;

			@Override
			public String call(Tuple2<String, Tuple2<Tuple2<Long, String>, String>> v1) throws Exception {

				long fristTime = v1._2()._1()._1();
				String url = v1._1();
				String summitTimeAndPv = v1._2()._1()._2();
				String cd = v1._2()._2();
				String summitTimeAndPvArr[] = summitTimeAndPv.split(Config.REPORT_KEY_DELIMITED);
				String reStr = url + " , frist appear time = " + DateUtil.getTimeStampFormat(fristTime, DateUtil.yyyy_MM_dd_HH_mm_ss) + ", summit time = " + summitTimeAndPvArr[0] + " , summit time pv = " + summitTimeAndPvArr[1] + " cd = " + cd;
				return reStr;

			}

		}).saveAsTextFile("/" + System.currentTimeMillis());

	}

}
