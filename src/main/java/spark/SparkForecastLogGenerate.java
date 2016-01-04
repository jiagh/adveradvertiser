package spark;

import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
import util.JsonUtil;
import util.PublicUtil;
import vo.log1_2.CogtuLog1_2;

public class SparkForecastLogGenerate {

	public static void main(String[] args) {

		JavaSparkContext ctx = PublicUtil.getJavaSparkContext("SparkForecastLogGenerate");
		//
		JavaRDD<String> sourceLog = null;

		//
		// ArrayList<HashMap<String, Object>> date =
		// mt.selectSql("SELECT day FROM merge_list ORDER BY day DESC LIMIT 7");
		// // 加载最近7天数据
		// for (int i = 0; i < date.size(); i++) {
		//
		// String tmp = date.get(i).get("day").toString();
		// String year = tmp.substring(0, 4);
		// String month = tmp.substring(4, 6);
		// String day = tmp.substring(6, 8);
		//
		// if (sourceLog == null)
		// sourceLog = ctx.textFile(Config.hdfsBakLogsReq + "/" + year + "/" +
		// month + "/" + day);
		// else
		// sourceLog.union(ctx.textFile(Config.hdfsBakLogsReq + "/" + year + "/"
		// + month + "/" + day));
		// }

		sourceLog = ctx.textFile("D://E/");

		// 按照 UID HASH 分组 保证相同数据在一个文件里
		sourceLog.mapToPair(new PairFunction<String, String, String>() {

			private static final long serialVersionUID = 4983154223504613638L;

			@Override
			public Tuple2<String, String> call(String t) throws Exception {

				try {

					CogtuLog1_2 cl = JsonUtil.toBean(t, CogtuLog1_2.class);
					return new Tuple2<String, String>(cl.getUid(), t);

				} catch (Exception e) {
					return null;
				}

			}
		}).partitionBy(new Partitioner() {

			private static final long serialVersionUID = 437528212374753841L;

			@Override
			public int numPartitions() {
				// TODO Auto-generated method stub
				return 8;
			}

			@Override
			public int getPartition(Object arg0) {
				// TODO Auto-generated method stub
				return Math.abs(arg0.toString().hashCode() % numPartitions());
			}

		}).sortByKey().map(new Function<Tuple2<String, String>, String>() {

			private static final long serialVersionUID = 1542224115712832012L;

			@Override
			public String call(Tuple2<String, String> v1) throws Exception {
				// TODO Auto-generated method stub
				return v1._2();
			}
		}).saveAsTextFile("D://D" + System.currentTimeMillis());
	}
}
