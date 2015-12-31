package jgh.hive;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.HiveContext;

import jgh.sql.SqlUtil;
import jgh.util.DateUtil;
import jgh.util.MysqlUtil;

/**
 * The Class SparkToHive.
 */
public class SparkToHive3 {

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
	    try {
		System.out.println("=================>"+sqlMap.get("extuceSql"));
		DataFrame dataFrame = sqlContext.sql(sqlMap.get("extuceSql"));
		SqlUtil.createTable(map);
		final StringBuilder insertSql = new StringBuilder();
		final HashMap<String, String> indexMap = getIndexMap(String.valueOf(map.get("index")));
		dataFrame.toJavaRDD().repartition(1).foreachPartition(new VoidFunction<Iterator<Row>>() {
		    int flag = 0;
		    @Override
		    public void call(Iterator<Row> t) throws Exception {
			while (t.hasNext()) {
			    Row row = (Row) t.next();
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
			    if (flag == 0) {
				insertSql.append("insert into offline_basic_analysis.report_").append(map.get("id")).append("(")
					.append(dispaly.substring(0, dispaly.lastIndexOf(","))).append(")").append(" values").append("(")
					.append(disValue.substring(0, disValue.lastIndexOf(","))).append(")");
			    } else {
				insertSql.append(",").append("(").append(disValue.substring(0, disValue.lastIndexOf(","))).append(")");
			    }
			    flag++;
			    if (flag % 100 == 0) {
				MysqlUtil db = new MysqlUtil();
				db.insertSql(insertSql.append(";").toString());
				insertSql.delete(0, insertSql.length());
				flag = 0;
			    }
			}
		    }
		});
		if (insertSql.length() > 0) {
		    MysqlUtil db = new MysqlUtil();
		    db.insertSql(insertSql.append(";").toString());
		}
		String sql = "update offline_basic_analysis.customize_report_info set creat_time='" + startTime + "',update_time='"
			+ DateUtil.timeStampToDate(System.currentTimeMillis()) + "' , status='Y' where id=" + map.get("id");
		mu.insertSql(sql);
	    } catch (Exception ex) {
		ex.printStackTrace();
		String sql = "update offline_basic_analysis.customize_report_info set creat_time='" + startTime + "',update_time='"
			+ DateUtil.timeStampToDate(System.currentTimeMillis()) + "' , status='E' where id=" + map.get("id");
		mu.insertSql(sql);
	    }

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
     * 获取统计项字段
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
