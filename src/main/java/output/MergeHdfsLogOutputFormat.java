package output;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;

public class MergeHdfsLogOutputFormat extends
		MergeHdfsLogMultipleOutputFormat<Text, Text> {

	@Override
	protected String generateFileNameForKeyValue(Text key, Text value,
			Configuration conf) {
		return key.toString();
	}
}
