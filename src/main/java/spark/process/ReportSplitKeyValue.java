package spark.process;

import java.io.Serializable;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;
import config.Config;

public class ReportSplitKeyValue implements Serializable {

	private static final long serialVersionUID = 4632888071925309797L;

	public JavaPairRDD<String, String> execute(JavaRDD<String> mergeLog) {

		JavaPairRDD<String, String> reportKeyValueGenerate = mergeLog.mapToPair(new PairFunction<String, String, String>() {

			private static final long serialVersionUID = 2078367817804688589L;

			@Override
			public Tuple2<String, String> call(String t) throws Exception {

				String tmp[] = t.split(Config.REPORT_VALUE_DELIMITED);

				if (tmp.length == 2)
					return new Tuple2<String, String>(tmp[0], tmp[1]);
				else
					return null;
			}

		});

		return reportKeyValueGenerate;
	}
}
