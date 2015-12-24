package jgh.hive;

import java.util.Arrays;
import java.util.HashMap;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.HiveContext;

import config.Config;
import jgh.output.SparkHiveOutputFormat;
import jgh.util.JsonUtil;
import jgh.vo.Result;
import scala.Tuple2;

public class SparkToHive2 {
    @SuppressWarnings("serial")
    public static void extuce(String args[], final String groupField, final String resultField, final String tableName, final HashMap<String, HashMap<String, String>> queryField,
	    final String orderField) {
	SparkConf conf = new SparkConf().setAppName("extuceHiveSql").setMaster("local[6]");
	JavaSparkContext sc = new JavaSparkContext(conf);
	HiveContext sqlContext = new HiveContext(sc.sc());
	// 拼接输出hdfs地址
	StringBuilder outPutBuilder = new StringBuilder();
	long t = System.currentTimeMillis();
	outPutBuilder.append(Config.NAMENODE_ADDRESS).append(Config.HDFS_LOG_MERGE_WORK).append("/").append(args[0]).append("-").append(t);
	String writePath = outPutBuilder.toString();
	System.out.println("hdfs output path：" + writePath);
	// 执行hiveSql
	DataFrame advData = sqlContext.sql(getConditionStr(groupField, resultField, tableName, queryField, orderField));
	advData.toJavaRDD().map(new Function<Row, Result>() {
	    public Result call(Row v1) throws Exception {
		Result result = new Result();
		if (groupField.length() > 0) {
		    result.setGroupField(groupField);
		} 
		if (orderField.length() > 0) {
		    result.setOrderField(orderField);
		}
		if (null != queryField && queryField.size() > 0) {
		    result.setQueryField(queryField.toString());
		}
		if (tableName.contains(".")) {
		    result.setSpace(tableName.split("\\.")[0]);
		    result.setTableName(tableName.split("\\.")[1]);
		} else {
		    result.setTableName(tableName);
		}
		HashMap<String, String> resultMap = new HashMap<String, String>();
		String field[] = resultField.split(",");
		String valuestr = v1.toString().replaceAll("\\[|\\]", "");
		String str[] = valuestr.split(",");
		boolean flag = valuestr.endsWith(",");
		for (int i = 0; i < field.length; i++) {
		    if (i == field.length - 1 && flag) {
			resultMap.put(field[i], "");
		    } else {
			resultMap.put(field[i], str[i]);
		    }
		}
		result.setResultMap(resultMap);
		return result;
	    }
	}).mapToPair(new PairFunction<Result, String, String>() {
	    @Override
	    public Tuple2<String, String> call(Result t) throws Exception {
		return new Tuple2<String, String>(t.getTableName(), JsonUtil.toJson(t));
	    }
	}).saveAsHadoopFile(writePath, String.class, String.class, SparkHiveOutputFormat.class);
	sc.stop();
	sc.close();
    }

    public static void main(String args[]) {

	String groupField = "country,province,city"; // 查询语句分组字段
	String resultField = "pv,click,country,province,city";
	String tableName = "advertisers";
	HashMap<String, HashMap<String, String>> queryField = new HashMap<String, HashMap<String, String>>();
	HashMap<String, String> queryTime = new HashMap<String, String>();
	queryTime.put("1", ">=");
	queryTime.put("4", "<=");
	queryField.put("reqType", queryTime);
	String orderField = "pv";
	extuce(args, groupField, resultField, tableName, queryField, orderField);
    }

    /**
     * 通过查询条件，展现项，分组等拼接查询语句
     * 
     * @param groupField
     * @param resultField
     * @param tableName
     * @param queryField
     * @param orderField
     * @return
     */
    public static String getConditionStr(String groupField, String resultField, String tableName, HashMap<String, HashMap<String, String>> queryField, String orderField) {
	StringBuilder queryBuilder = new StringBuilder();
	StringBuilder sqlBuilder = new StringBuilder();
	sqlBuilder.append("select ");
	String result[] = resultField.split(",");
	for (int i = 0; i < result.length; i++) {
	    if (result[i].equals("pv")) {
		result[i] = "sum(case when reqType=2 then 1 else 0 end) as pv ";
	    } else if (result[i].equals("click")) {
		result[i] = "sum(case when reqType=3 then 1 else 0 end) as click ";
	    }
	}
	sqlBuilder.append(Arrays.toString(result).replaceAll("\\[|\\]", ""));
	sqlBuilder.append(" from ").append(tableName).append(" ");
	for (String query : queryField.keySet()) {
	    HashMap<String, String> queryMap = queryField.get(query);
	    for (String key : queryMap.keySet()) {
		queryBuilder.append(query).append(queryMap.get(key)).append(" '").append(key).append("' and ");
	    }
	}
	if (queryBuilder.length() > 0) {
	    sqlBuilder.append(" where ").append(queryBuilder.substring(0, queryBuilder.lastIndexOf("and ")));
	}
	if (groupField.length() > 0) {
	    sqlBuilder.append(" group by ").append(groupField);
	}
	if (orderField.length() > 0) {
	    sqlBuilder.append(" order by ").append(orderField).append(" desc");
	}
	System.out.println("sql==>" + sqlBuilder.toString());
	return sqlBuilder.toString();
    }

    /**
     * 获取原始点击文件中的小时,用于分区
     * 
     * @param hourstr
     * @return
     */
    public static int getHour(String hourstr) {
	if (hourstr.startsWith("0")) {
	    hourstr = hourstr.replace("0", "");
	}
	return Integer.parseInt(hourstr);
    }

}
