package output;

import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;

public class ReportSparkOutputFormat extends MultipleTextOutputFormat<String, String> {

	@Override
	protected String generateFileNameForKeyValue(String key, String value, String name) {

		// TODO Auto-generated method stub
		return key.split(" == ")[0];
	}
}