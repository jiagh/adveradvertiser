package spark.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.HiveContext;

/**
 * 获取没有点击广告的用户的最近的所有浏览轨迹
 */
public class NoClickUidSample {

	public static void main(String[] args) {

		if (args.length < 3) {
			System.err.println("Usage: NoClickUidSample <dateCondition> <partitionNum> <sampleFraction> ");
			System.exit(1);
		}

		String dateCondition = args[0];

		SparkConf sparkConf = new SparkConf().setAppName("NoClickUidSample");
		SparkContext sc = new SparkContext(sparkConf);
		JavaSparkContext ctx = new JavaSparkContext(sc);
		HiveContext hc = new HiveContext(sc);

		// TEMPORARY UDF
		hc.sql("CREATE TEMPORARY FUNCTION reqs_parse_array_udf AS 'hive.udf.ReqsParseArrayListUdf'");
		hc.sql("CREATE TEMPORARY FUNCTION sort_array_timestamp_return_url_array_udf AS 'hive.udf.SortArrayTimeStampReturnUrlArrayUdf'");

		// 点击过的用户 去重
		DataFrame A = hc.sql("SELECT uid FROM adnet_da_report.cogtu_log_view WHERE " + dateCondition + "  AND reqType = 3 GROUP BY uid");
		// 所有用户 去重
		DataFrame B = hc.sql("SELECT uid,pageUri,timestamp FROM adnet_da_report.cogtu_log_view WHERE " + dateCondition + " AND reqType = 1 ");
		// 排除点击过广告的用户
		DataFrame C = B.select(B.col("uid").as("cuid")).except(A).sample(true, Double.parseDouble(args[2]));
		// 获取用户所有访问URL
		DataFrame D = C.join(B, B.col("uid").equalTo(C.col("cuid"))).select("uid", "pageUri", "timestamp");
		D.registerTempTable("D");
		hc.sql("SELECT uid,sort_array_timestamp_return_url_array_udf(COLLECT_LIST(CONCAT_WS('|-|',timestamp,pageUri))) FROM D GROUP BY uid").toJavaRDD().map(new Function<Row, String>() {

			/**
			*
			*/
			private static final long serialVersionUID = 340763719139685786L;

			@Override
			public String call(Row v1) throws Exception {
				return v1.toString().replaceAll("\\[|\\]", "");
			}

		}).repartition(Integer.parseInt(args[1])).saveAsTextFile("/sparkSqlResult/noClickUidSample-" + System.currentTimeMillis());

	}
}
