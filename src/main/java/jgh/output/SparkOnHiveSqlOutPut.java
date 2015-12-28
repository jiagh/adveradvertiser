package jgh.output;

import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;

public class SparkOnHiveSqlOutPut extends MultipleTextOutputFormat<String, String> {

	@Override
	protected String generateFileNameForKeyValue(String key, String value, String name) {
		return  key;
	}

	@Override
	protected String generateActualKey(String key, String value) {
		// TODO Auto-generated method stub
		return super.generateActualKey(null, value);
	}

}