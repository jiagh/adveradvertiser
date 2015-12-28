package jgh.hive;

import java.util.ArrayList;
import java.util.HashMap;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.HiveContext;

import jgh.sql.SqlUtil;
import jgh.util.DateUtil;
import jgh.util.MysqlUtil;

// TODO: Auto-generated Javadoc
/**
 * The Class SparkToHive.
 */
public class SparkToHive {

    /**
     * 加载hdfs文件，进行报表统计，写入mysql
     */
    @SuppressWarnings("serial")
    public static void extuceHiveSql() {
	SparkConf sparkConf = new SparkConf().setAppName("JavaSparkSQL").setMaster("local[8]");
	JavaSparkContext ctx = new JavaSparkContext(sparkConf);
	HiveContext sqlContext = new HiveContext(ctx.sc());
	MysqlUtil mu = new MysqlUtil();
	ArrayList<HashMap<String, Object>> list = mu.selectSql("select * from offline_basic_analysis.customize_report_info where status='N' ");
	for (final HashMap<String, Object> map : list) {
	    Long t = System.currentTimeMillis();
	    String startTime = DateUtil.timeStampToDate(t);
	    HashMap<String, String> sqlMap = SqlUtil.builderSql(map);
	    final String display[] = sqlMap.get("resultField").split(",");
	    DataFrame dataFrame = sqlContext.sql(sqlMap.get("extuceSql"));
	    Row[] rowList = dataFrame.collect();
	    SqlUtil.createTable(map);
	    HashMap<String, String> indexMap = getIndexMap(String.valueOf(map.get("index")));
	 
	    ArrayList<String> sqlList = new ArrayList<String>();
	    for (Row row : rowList) {
		StringBuilder insertSql = new StringBuilder();
		StringBuilder displayBuilder = new StringBuilder();
		StringBuilder disValueBuilder = new StringBuilder();
		for (int i = 0; i < display.length; i++) {
		    displayBuilder.append(display[i]).append(",");
		    if (indexMap.containsKey(display[i])) {
			disValueBuilder.append(row.get(i)).append(",");
		    } else {
			disValueBuilder.append("'").append(row.get(i)).append("'").append(",");
		    }
		}
		String dispaly = displayBuilder.toString();
		String disValue = disValueBuilder.toString();
		insertSql.append("insert into offline_basic_analysis.report_").append(map.get("id")).append("(").append(dispaly.substring(0, dispaly.lastIndexOf(","))).append(")").append(" values")
			.append("(").append(disValue.substring(0, disValue.lastIndexOf(","))).append(");");
		System.out.println(insertSql);
		sqlList.add(insertSql.toString());
	    }
	    mu.insertListSql(sqlList);
	    String endTime = DateUtil.timeStampToDate(System.currentTimeMillis());
	    String sql="update offline_basic_analysis.customize_report_info set creat_time='" + startTime + "',update_time='" + endTime + "' , status='Y' where id=" + map.get("id");
	    mu.insertSql(sql);
	}
	ctx.stop();
	ctx.close();
    }

    /**
     * The main method.
     *
     * @param args
     *            the arguments
     */
    public static void main(String args[]) {
	extuceHiveSql();
    }

    /**
     * Gets the index map.
     *
     * @param index
     *            the index
     * @return the index map
     */
    public static HashMap<String, String> getIndexMap(String index) {
	String str[] = index.split(",");
	HashMap<String, String> indexMap = new HashMap<String, String>();
	for (String s : str) {
	    indexMap.put(s, "");
	}
	return indexMap;
    }

}
