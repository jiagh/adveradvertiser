package output;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.lib.MultipleOutputFormat;
import org.apache.hadoop.util.Progressable;

import util.DateUtil;
import util.JsonUtil;
import vo.log1_2.CogtuLog1_2;
import vo.log1_2.ReqLog1_2;

public class ReqLogInitSparkOutputFormat extends MultipleOutputFormat<String, String> {

	@Override
	protected String generateFileNameForKeyValue(String key, String value, String name) {

		CogtuLog1_2 cl = JsonUtil.toBean(key.toString(), CogtuLog1_2.class);

		// 判断是否REQLOG
		if (cl instanceof ReqLog1_2) {

			ReqLog1_2 rl = (ReqLog1_2) cl;

			String type = "err";

			if (rl.getReqType() == 1)
				type = "fetch";
			else if (rl.getReqType() == 2)
				type = "imp";
			else if (rl.getReqType() == 3)
				type = "click";

			return type + "_" + DateUtil.getTimeStampFormat(rl.getTimestamp(), DateUtil.yyyyMMddHH) + "_" + name;

		} else
			return "ERR";

	}

	@Override
	protected RecordWriter<String, String> getBaseRecordWriter(FileSystem fs, JobConf job, String name,
			Progressable arg3) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected String generateActualKey(String key, String value) {
		// TODO Auto-generated method stub
		return super.generateActualKey(null, value);
	}

}