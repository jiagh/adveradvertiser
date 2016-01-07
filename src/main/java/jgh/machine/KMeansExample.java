package jgh.machine;

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
    public static void main(String[] args) {
	kmeansClass();
    }

    public static <T> void print(Collection<T> c) {
	for (T t : c) {
	    System.out.println(t.toString());
	}
    }
    /**
     * 聚类数据
     * 
     * @return
     */
    @SuppressWarnings("serial")
    public static List<String> kmeansClass() {
	SparkConf sparkConf = new SparkConf().setAppName("K-Means").setMaster("local[2]");
	String path = "/jgh/mllib/kmeans_data.txt";
	JavaSparkContext sc = new JavaSparkContext(sparkConf);
	JavaRDD<Vector> parsedData = sc.textFile(path).map(new Function<String, Vector>() {
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
	List<String> listInfo = (List<String>) parsedData.map(v -> String.valueOf(clusters.predict(v))).collect();
	clusters.predict(Vectors.dense(new double[] { 1.1, 2.1, 3.1 }));
	print(listInfo);
	sc.stop();
	sc.close();
	return listInfo;
    }

    /**
     * 循环输出中心点与预测
     * 
     * @param clusters
     */
    public static void getCenterPoint(KMeansModel clusters) {
	for (Vector center : clusters.clusterCenters()) {
	    System.out.println("uuuuu" + center);
	}
	// 预测 1.1, 2.1, 3.1所属分类
	System.out.println("Prediction of (1.1, 2.1, 3.1): " + clusters.predict(Vectors.dense(new double[] { 1.1, 2.1, 3.1 })));
    }
}
