package spark.process;

import java.io.Serializable;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;

import config.Config;
import output.TmpDataSparkOutputFormat;
import scala.Tuple2;

public class ReportKeyValueOutPut implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 3146679418821488094L;

	public void execute(JavaPairRDD<String, String> reportValueMerge, final Broadcast<String> analysisFolderName, String outTmpData) {

		// 报表累加数据输出到HDFS按照日期命名
		reportValueMerge.mapToPair(new PairFunction<Tuple2<String, String>, String, String>() {

			private static final long serialVersionUID = 5336334610895738817L;

			@Override
			public Tuple2<String, String> call(Tuple2<String, String> t) throws Exception {

				return new Tuple2<String, String>(t._1().split("\"day\":\"")[1].split("\"")[0] + "-" + analysisFolderName.getValue(), t._1() + "\t" + t._2());
			}
		}).repartition(Config.ORGANIZA_INSERTSQL_NUM_PARTITIONS).saveAsHadoopFile(outTmpData, String.class, String.class, TmpDataSparkOutputFormat.class);
	}
}
