package output;

import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;

public class KafkaDataToHdfsOutputFormat extends MultipleTextOutputFormat<String, String> {

	@Override
	protected String generateFileNameForKeyValue(String key, String value, String name) {
		return key + "-" + name.split("-")[1];
	}

	@Override
	protected String generateActualKey(String key, String value) {
		return super.generateActualKey(null, value);
	}

}