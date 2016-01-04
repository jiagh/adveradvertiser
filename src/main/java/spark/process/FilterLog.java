package spark.process;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import util.MysqlUtil;

public class FilterLog implements Serializable {

	/**
	 * 从数据库取得过滤规则
	 */
	public static ArrayList<String> getFilterRule() {
		MysqlUtil db = new MysqlUtil();
		ArrayList<HashMap<String, Object>> al = db.selectSql("SELECT rule FROM filter_log_rule");
		ArrayList<String> re = new ArrayList<String>();
		for (int i = 0; i < al.size(); i++) {
			re.add(al.get(i).get("rule").toString());
		}
		db.close();
		return re;
	}

	/**
	 * 执行过滤
	 */
	private static final long serialVersionUID = 4208646060212596534L;

	public JavaRDD<String> execute(JavaRDD<String> sourceLog) {

		JavaRDD<String> re = sourceLog.filter(new Function<String, Boolean>() {

			ArrayList<String> filterRule = getFilterRule();

			/**
			 * 
			 */
			private static final long serialVersionUID = -6392068556058270405L;

			@Override
			public Boolean call(String v1) throws Exception {

				for (int i = 0; i < filterRule.size(); i++) {
					if (v1.toLowerCase().indexOf(filterRule.get(i).toString().toLowerCase()) != -1) {
						return false;
					}
				}
				return true;
			}
		});
		return re;
	}
}
