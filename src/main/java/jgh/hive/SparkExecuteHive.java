package jgh.hive;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.HiveContext;

public class SparkExecuteHive {

    public static void sparkExecuteHive(){
	SparkConf conf = new SparkConf().setAppName("extuceHiveSql").setMaster("local[6]");
	JavaSparkContext sc = new JavaSparkContext(conf);
	HiveContext sqlContext = new HiveContext(sc.sc());
	DataFrame dataFrame=sqlContext.sql("select count(*) from jgh.adv");
	List<Row> list=dataFrame.collectAsList();
	System.out.println("count===:"+list.get(0).getInt(0));
	sc.stop();
	sc.close();
    }
    
    public static void main(String args[]){
	sparkExecuteHive();
    }
}
