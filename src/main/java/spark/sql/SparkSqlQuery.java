package spark.sql;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.HiveContext;

/**
 * 用于SQL查询生成结果到HDFS
 */
public class SparkSqlQuery {

	public static void main(String[] args) {

		if (args.length < 2) {
			System.err.println("Usage: SparkSqlQuery <sql> <partitionNum> <outputName>(optional)");
			System.exit(1);
		}

		String outputName = "";

		if (args.length == 3) {
			outputName = args[2];
		} else {
			outputName = System.currentTimeMillis() + "";
		}

		SparkConf sparkConf = new SparkConf().setAppName("SparkSqlQuery");
		SparkContext sc = new SparkContext(sparkConf);
		JavaSparkContext ctx = new JavaSparkContext(sc);
		HiveContext hc = new HiveContext(sc);

		hc.sql("CREATE TEMPORARY FUNCTION reqs_parse_array_udf AS 'hive.udf.ReqsParseArrayListUdf'");
		hc.sql("CREATE TEMPORARY FUNCTION sort_array_timestamp_return_url_array_udf AS 'hive.udf.SortArrayTimeStampReturnUrlArrayUdf'");
		hc.sql(args[0]).repartition(Integer.parseInt(args[1])).toJavaRDD().map(new Function<Row, String>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 340763719139685786L;

			@Override
			public String call(Row v1) throws Exception {
				return v1.toString().replaceAll("\\[|\\]", "");
			}

		}).repartition(Integer.parseInt(args[1])).saveAsTextFile("/sparkSqlResult/" + outputName);
	}
}
