package spark.process;

import java.io.Serializable;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import config.Config;
import handle.report.AbstractHandleReport;
import scala.Tuple2;

public class ReportValueMerge implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -8035705335147749924L;

	public JavaPairRDD<String, String> reportValueMerge;

	public JavaPairRDD<String, Iterable<String>> reportValueMergeGroupBy;

	public JavaPairRDD<String, String> getReportValueMerge() {
		return reportValueMerge;
	}

	public void setReportValueMerge(JavaPairRDD<String, String> reportValueMerge) {
		this.reportValueMerge = reportValueMerge;
	}

	public JavaPairRDD<String, Iterable<String>> getReportValueMergeGroupBy() {
		return reportValueMergeGroupBy;
	}

	public void setReportValueMergeGroupBy(JavaPairRDD<String, Iterable<String>> reportValueMergeGroupBy) {
		this.reportValueMergeGroupBy = reportValueMergeGroupBy;
	}

	public void execute(JavaPairRDD<String, String> reportKeyValueGenerate, JavaSparkContext ctx, String outTmpData) {

		// 累计VALUE
		reportValueMerge = reportKeyValueGenerate.reduceByKey(new Function2<String, String, String>() {

			private static final long serialVersionUID = -5235615227793944246L;

			@Override
			public String call(String arg0, String arg1) throws Exception {

				String a0[] = arg0.split(Config.REPORT_KEY_DELIMITED);
				String a1[] = arg1.split(Config.REPORT_KEY_DELIMITED);

				//
				String key = a0[0];
				// 类名
				String className = "";

				if (key.toString().toLowerCase().indexOf("admin_") != -1)
					className = "admin." + key.split(Config.REPORT_KEY_DELIMITED)[0];
				else if (key.toString().toLowerCase().indexOf("advertisers_") != -1)
					className = "advertisers." + key.split(Config.REPORT_KEY_DELIMITED)[0].split("_", 2)[1];
				else if (key.toString().toLowerCase().indexOf("publisher_") != -1)
					className = "publisher." + key.split(Config.REPORT_KEY_DELIMITED)[0].split("_", 2)[1];

				// 抽象类
				AbstractHandleReport ahr = (AbstractHandleReport) Class.forName("handle.report." + className).newInstance();
				// 累加
				return ahr.outReduce(key, a0[1], a1[1]);
			}
		});

		// 按照报表名称分组
		JavaPairRDD<String, Iterable<String>> reportValueMergeGroupBy = reportValueMerge.mapToPair(new PairFunction<Tuple2<String, String>, String, String>() {

			private static final long serialVersionUID = -122593330968263741L;

			@Override
			public Tuple2<String, String> call(Tuple2<String, String> t) throws Exception {

				String tmp1[] = t._1().split(Config.REPORT_KEY_DELIMITED);
				String tmp2[] = t._2().split(Config.REPORT_KEY_DELIMITED);

				return new Tuple2<String, String>(tmp1[0], tmp1[1] + Config.REPORT_VALUE_DELIMITED + tmp2[1]);
			}
		}).groupByKey();
		
		setReportValueMerge(reportValueMerge);
		setReportValueMergeGroupBy(reportValueMergeGroupBy);

	}
}
