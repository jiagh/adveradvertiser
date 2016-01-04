package output;

import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;

public class MergeHdfsLogSparkOutputFormat extends MultipleTextOutputFormat<String, String> {

	@Override
	protected String generateFileNameForKeyValue(String key, String value, String name) {
		return key + "-" + name;
	}

	@Override
	protected String generateActualKey(String key, String value) {
		return super.generateActualKey(null, value);
	}

}