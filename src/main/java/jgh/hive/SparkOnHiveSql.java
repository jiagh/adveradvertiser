package jgh.hive;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.URI;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.SQLContext;

import config.Config;
import jgh.util.HiveUtil;
import jgh.util.JsonUtil;
import jgh.vo.Result;

public class SparkOnHiveSql {

    /**
     * 生成曝光与点击数据，写入hdfs
     * 
     * @param writePath
     * @param writerData
     */
    public void writeHdfs(String writePath, String writerData) {
	try {
	    FileSystem fs = FileSystem.get(URI.create(Config.NAMENODE_ADDRESS), new Configuration());
	    FSDataOutputStream hdfsWrite = fs.create(new Path(writePath), true);
	    Writer out = new OutputStreamWriter(hdfsWrite, "utf-8");
	    out.write(writerData);
	    out.close();
	    fs.close();
	} catch (IOException e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
	}

    }

    /**
     * 程序执行入口
     * 
     * @param args
     */
    public static void main(String args[]) {
	SparkOnHiveSql shs = new SparkOnHiveSql();
	JavaSparkContext jsc = new JavaSparkContext();

//	HashMap<String, HashMap<String, String>> queryField = new HashMap<String, HashMap<String, String>>();
//	HashMap<String, String> queryStartTime = new HashMap<String, String>();
//	queryStartTime.put("11", ">=");
//	HashMap<String, String> queryEndTime = new HashMap<String, String>();
//	queryEndTime.put("14", "<=");
//	queryField.put("hour", queryStartTime);
//	queryField.put("hour", queryEndTime);
//	// queryField.put("hour", "11"); //查询语句条件项
//	String groupField = "date,country,province,city"; // 查询语句分组字段
//	String resultField = "pv,click,date,country,province,city"; // 查询语句展现项
//	String tableName = "jgh.advertisers";// 表空间与表名
//	String orderField = "pv";// 排序字段。默认desc
//	shs.hiveSql(groupField, resultField, tableName, queryField, orderField);
	shs.extuce();
    }

    /**
     * 执行查询hive语句
     * 
     * @param groupField
     * @param resultField
     * @param tableName
     * @param queryField
     * @param orderField
     */
    public void hiveSql(String groupField, String resultField, String tableName, HashMap<String, HashMap<String, String>> queryField, String orderField) {
	try {
	    Connection conn = HiveUtil.getConn();
	    Statement stm = conn.createStatement();
	    ResultSet rs = stm.executeQuery(getConditionStr(groupField, resultField, tableName, queryField, orderField));
	    StringBuilder writeBuilder = new StringBuilder();
	    while (rs.next()) {
		Result res = new Result();
		String tableStr[] = tableName.split("\\.");
		res.setSpace(tableStr[0]);
		res.setTableName(tableStr[1]);
		if (queryField.size() > 0) {
		    res.setQueryField(queryField.toString());
		}
		HashMap<String, String> resultMap = new HashMap<String, String>();
		for (String result : resultField.split(",")) {
		    if (result.trim().endsWith("as pv")) {
			resultMap.put("pv", rs.getString("pv"));
		    } else if (result.trim().endsWith("as click")) {
			resultMap.put("click", rs.getString("click"));
		    } else {
			resultMap.put(result, rs.getString(result.toLowerCase()));
		    }
		}
		res.setResultMap(resultMap);
		if (groupField.length() > 0) {
		    res.setGroupField(groupField);
		}
		if (orderField.length() > 0) {
		    res.setOrderField(orderField);
		}
		writeBuilder.append(JsonUtil.toJson(res)).append("\n");
	    }
	    System.out.println(writeBuilder.toString());

	    String writePath = Config.NAMENODE_ADDRESS + Config.HDFS_LOG_MERGE_WORK + "/" + System.currentTimeMillis();
	    writeHdfs(writePath, writeBuilder.toString());
	} catch (Exception e) {
	    e.printStackTrace();
	}
    }

    // public void extuceSql(String groupField, String resultField, String
    // tableName, HashMap<String, HashMap<String, String>> queryField, String
    // orderField) {
    // try {
    //// SparkConf conf = new SparkConf().setAppName("extuceHiveSql");
    //// JavaSparkContext sc = new JavaSparkContext(conf);
    //// HiveContext sqlContext = new HiveContext(sc.sc());
    //// DataFrame df = sqlContext.sql(getConditionStr(groupField, resultField,
    // tableName, queryField, orderField));
    // df.javaRDD().map(new Function<Row, Result>() {
    // @Override
    // public Result call(Row v1) throws Exception {
    //// v1.getst
    // return null;
    // }
    // });
    // sc.stop();
    // sc.close();
    //
    // } catch (Exception e) {
    // e.printStackTrace();
    // }
    // }

    @SuppressWarnings("serial")
    public void extuce() {
	SparkConf sparkConf = new SparkConf().setAppName("JavaSparkSQL").setMaster("local[2]");
	JavaSparkContext ctx = new JavaSparkContext(sparkConf);
	SQLContext sqlContext = new SQLContext(ctx);
	String writePath =Config.NAMENODE_ADDRESS+"/hive/warehouse/jgh.db/advertisers/date=2015-12-03";
	//JavaRDD<Advertisers> adver = 
	ctx.textFile(writePath).foreach(new VoidFunction<String>() {
	    
	    @Override
	    public void call(String t) throws Exception {
		System.out.println("values==>"+t);
		
	    }
	});
//	    .map(new Function<String, Advertisers>() {
//	    @Override
//	    public Advertisers call(String line) {
//		System.out.println("values==>"+line);
////		String[] str = line.split("\\$");
////		Advertisers adv = AdvertisersTookit.getAdvertisers(str);
//		return null;
//	    }
//	}).collect();
//	DataFrame dataFrame = sqlContext.createDataFrame(adver, Advertisers.class);
//	dataFrame.registerTempTable("advertisers");
//	System.out.println("=======>"+dataFrame.count());
	ctx.stop();
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
		result[i] = "sum(case when reqtype=2 then 1 else 0 end) as pv ";
	    } else if (result[i].equals("click")) {
		result[i] = "sum(case when reqtype=3 then 1 else 0 end) as click ";
	    }
	}

	sqlBuilder.append(Arrays.toString(result).replaceAll("\\[|\\]", ""));
	sqlBuilder.append(" from ").append(tableName).append(" ");
	for (String query : queryField.keySet()) {
	    HashMap<String, String> queryMap = queryField.get(query);
	    for (String key : queryMap.keySet()) {
		queryBuilder.append(query).append(queryMap.get(query)).append(" '").append(key).append("' and ");
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
}
