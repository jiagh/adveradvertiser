package jgh.cassandra;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;
import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapToRow;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.spark.connector.japi.CassandraRow;

import jgh.util.CassandraUtil;
import jgh.util.JsonUtil;
import jgh.vo.AdLog1_1;
import jgh.vo.CogtuLog1_1;
import jgh.vo.ReqLog1_1;
import scala.Tuple2;


public class SparkCassandra {

    public static void main(String args[]) {
	// getDataCassandra();
	// getCount();
	// String createSpaceSql = "CREATE KEYSPACE testspace WITH REPLICATION
	// ={ 'class' : 'SimpleStrategy', 'replication_factor' : 1 }";
	String createTaleSql = "CREATE TABLE testspace.adv_pv_click_info (advid text PRIMARY KEY,pv counter,click counter)";
	// "drop table testspace.adv_pv_click_info"
	// createTableOrSpace(createTaleSql);
	getAdvPvClick();
    }

    public static void getDataCassandra() {
	// CassandraUtil caClient = new CassandraUtil();
	// caClient.connect("172.16.60.1");
	// Session session = caClient.getSession();
	// PreparedStatement statement = session.prepare("SELECT * FROM
	// testspace.adv_pv_click_info ");
	// BoundStatement boundStatement = new BoundStatement(statement);
	// ResultSet rs = session.execute(boundStatement);
	// ArrayList<Row> calist = (ArrayList<Row>) rs.all();
	// for(Row row:calist){
	// System.out.format("advid:%s pv:%d click:%d %n",
	// row.getString("advid"),row.getInt("pv"),row.getInt("click"));
	// }
	// caClient.close();
	SparkConf sparkConf = new SparkConf().set("spark.cassandra.connection.host", "172.16.60.1").set("spark.cleaner.ttl", "3600").setAppName("Test");
	sparkConf.setMaster("local[8]");
	JavaSparkContext ctx = new JavaSparkContext(sparkConf);
	JavaRDD<String> cassandraRowsRDD = javaFunctions(ctx).cassandraTable("testspace", "adv_pv_click_info").map(new Function<CassandraRow, String>() {
	    @Override
	    public String call(CassandraRow cassandraRow) throws Exception {
		System.out.println(cassandraRow.toString());
		return cassandraRow.toString();
	    }
	});
	cassandraRowsRDD.count();
	ctx.stop();
	ctx.close();
    }

    public static void createTableOrSpace(String sql) {
	CassandraUtil caClient = new CassandraUtil();
	caClient.connect("172.16.60.1");
	Session session = caClient.getSession();
	PreparedStatement statement = session.prepare(sql);
	BoundStatement boundStatement = new BoundStatement(statement);
	session.execute(boundStatement);
	System.out.println("sql extuce success!!");
	caClient.close();
    }

    public static void insertCassandra(List<Tuple2<String, ArrayList<Integer>>> tupleList) {
	CassandraUtil caClient = new CassandraUtil();
	caClient.connect("172.16.60.1");
	Session session = caClient.getSession();
	BatchStatement batchStatement = new BatchStatement();
	for (Tuple2<String, ArrayList<Integer>> t : tupleList) {
	    ArrayList<Integer> value = t._2();
	    Insert insert = QueryBuilder.insertInto("testspace", "adv_pv_click_info").values(new String[] { "advid", "pv", "click" },
		    new Object[] { t._1(), value.get(0), value.get(1) });
	    batchStatement.add(insert);
	}
	session.execute(batchStatement);
	System.out.println("insert table success!!");
	caClient.close();
    }


    @SuppressWarnings("serial")
    public static void getAdvPvClick() {
	SparkConf sparkConf = new SparkConf().set("spark.cassandra.connection.host", "172.16.60.1").set("spark.cleaner.ttl", "3600").setAppName("Test");
	sparkConf.setMaster("local[8]");
	JavaSparkContext ctx = new JavaSparkContext(sparkConf);
	String inputFile = "D:\\tusmiple\\work file\\testdata";
	// String inputFile = Config.NAMENODE_ADDRESS + Config.HDFS_LOG_BAK +
	// "/2015/12/02";
	JavaRDD<String> rdd = ctx.textFile(inputFile, 2);
	
	List<Tuple2<String, ArrayList<Integer>>> tupleList = rdd.flatMapToPair(new PairFlatMapFunction<String, String, ArrayList<Integer>>() {
	    @Override
	    public Iterable<Tuple2<String, ArrayList<Integer>>> call(String t) throws Exception {
		CogtuLog1_1 cl = JsonUtil.toBean(t.toString(), CogtuLog1_1.class);
		ArrayList<Tuple2<String, ArrayList<Integer>>> re = new ArrayList<Tuple2<String, ArrayList<Integer>>>();
		ArrayList<Integer> keycount = new ArrayList<Integer>(Arrays.asList(0, 0));
		if (cl instanceof ReqLog1_1) {
		    ReqLog1_1 r1 = JsonUtil.toBean(t, ReqLog1_1.class);
		    for (AdLog1_1 log : r1.getReqs()) {
			if (r1.getReqType() == 2) {
			    keycount.set(0, keycount.get(0) + 1);
			    re.add(new Tuple2<String, ArrayList<Integer>>(log.getProjectId() + "-" + log.getCampaignId() + "-" + log.getCreativeId(), keycount));
			} else if (r1.getReqType() == 3) {
			    keycount.set(1, keycount.get(1) + 1);
			    re.add(new Tuple2<String, ArrayList<Integer>>(log.getProjectId() + "-" + log.getCampaignId() + "-" + log.getCreativeId(), keycount));
			}
		    }
		}
		return re;
	    }
	}).reduceByKey(new Function2<ArrayList<Integer>, ArrayList<Integer>, ArrayList<Integer>>() {
	    @Override
	    public ArrayList<Integer> call(ArrayList<Integer> v1, ArrayList<Integer> v2) throws Exception {
		// TODO Auto-generated method stub
		ArrayList<Integer> value = new ArrayList<Integer>();
		for (int i = 0; i < v1.size(); i++) {
		    int v = v1.get(i).intValue();
		    int vs = v2.get(i).intValue();
		    value.add(v + vs);
		}
		return value;
	    }
	}).collect();

	ArrayList<AdvInfo> adv = new ArrayList<AdvInfo>();
	for (Tuple2<String, ArrayList<Integer>> t : tupleList) {
	    ArrayList<Integer> value = t._2();
	    System.out.format("advid:%s pv:%d click:%d %n", t._1(), value.get(0), value.get(1));
	    adv.add(new AdvInfo(t._1(), value.get(0), value.get(1)));
	}

	JavaRDD<AdvInfo> advInfo = ctx.parallelize(adv);
	javaFunctions(advInfo).writerBuilder("testspace", "adv_pv_click_info", mapToRow(AdvInfo.class)).saveToCassandra();
	//insertCassandra(tupleList);
	ctx.stop();
	ctx.close();
    }

    @SuppressWarnings("serial")
    public static class AdvInfo implements Serializable {
	private String advid;
	private int pv;
	private int click;

	public AdvInfo(String advid, int pv, int click) {
	    this.advid = advid;
	    this.pv = pv;
	    this.click = click;
	}

	public String getAdvid() {
	    return advid;
	}

	public void setAdvid(String advid) {
	    this.advid = advid;
	}

	public int getPv() {
	    return pv;
	}

	public void setPv(int pv) {
	    this.pv = pv;
	}

	public int getClick() {
	    return click;
	}

	public void setClick(int click) {
	    this.click = click;
	}

    }
}
