package jgh.kmeans;

import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

import java.util.Collection;
import java.util.List;

import org.apache.spark.SparkConf;

public class KMeansExample {
    @SuppressWarnings("serial")
    public static void main(String[] args) {
	SparkConf sparkConf = new SparkConf().setAppName("K-Means").setMaster("local[2]");
	String path = "/jgh/mllib/kmeans_data.txt";
	JavaSparkContext sc = new JavaSparkContext(sparkConf);
	JavaRDD<String> data = sc.textFile(path);
	JavaRDD<Vector> parsedData = data.map(new Function<String, Vector>() {
	    public Vector call(String s) {
		String[] sarray = s.split(" ");
		double[] values = new double[sarray.length];
		for (int i = 0; i < sarray.length; i++)
		    values[i] = Double.parseDouble(sarray[i]);
		return Vectors.dense(values);
	    }
	});
	parsedData.cache(); 

	int numClusters = 2; // 预测分为2个簇类
	int numIterations = 20; // 迭代20次
	int runs = 10; // 运行10次，选出最优解
	KMeansModel clusters = KMeans.train(parsedData.rdd(), numClusters, numIterations, runs);
	// 计算测试数据分别属于那个簇类
//	print();
	List<String> listInfo=parsedData.map(v -> v.toString() +String.valueOf(clusters.predict(v))).collect();
//	List<String> listInfo=parsedData.map(new Function<Vector, String>() {
//
//	    @Override
//	    public String call(Vector v1) throws Exception {
//		// TODO Auto-generated method stub
//		return String.valueOf(clusters.predict(v1));
//	    }
//	}).collect();
//	
	print(listInfo);
	// 计算cost
//	double wssse = clusters.computeCost(parsedData.rdd());
//	System.out.println("Within Set Sum of Squared Errors = " + wssse);
//
//	// 打印出中心点
//	System.out.println("Cluster centers:");
//	for (Vector center : clusters.clusterCenters()) {
//	    System.out.println("uuuuu" + center);
//	}
////	// 进行一些预测
	System.out.println("Prediction of (1.1, 2.1, 3.1): " + clusters.predict(Vectors.dense(new double[] { 1.1, 2.1, 3.1 })));
	System.out.println("Prediction of (10.1, 9.1, 11.1): " + clusters.predict(Vectors.dense(new double[] { 8.1, 9.1, 7.6 })));
//	System.out.println("Prediction of (8.1, 9.1, 7.6): " + clusters.predict(Vectors.dense(new double[] { 8.1, 9.1, 7.6 })));
	sc.stop();
	sc.close();
    }

    public static <T> void print(Collection<T> c) {
	for (T t : c) {
	    System.out.println(t.toString());
	}
    }
}
