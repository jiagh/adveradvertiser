package jgh.machine;


import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.linalg.SingularValueDecomposition;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.linalg.distributed.RowMatrix;

public class SvdExample {
    public static void main(String[] args) {
	SparkConf conf = new SparkConf().setAppName("SVDTest").setMaster("local[2]");

	JavaSparkContext sc = new JavaSparkContext(conf);
	JavaRDD<String> data = sc.textFile("/jgh/mllib/svd.txt");
	JavaRDD<Vector> rows = data.map(s -> {
	    double[] values = Arrays.asList(s.split(" ")).stream().mapToDouble(Double::parseDouble).toArray();
	    return Vectors.dense(values);
	});

	RowMatrix mat = new RowMatrix(rows.rdd());
	// 第一个参数3意味着取top 3个奇异值，第二个参数true意味着计算矩阵U，第三个参数意味小于1.0E-9d的奇异值将被抛弃
	SingularValueDecomposition<RowMatrix, Matrix> svd = mat.computeSVD(3, true, 1.0E-9d);
	RowMatrix U = svd.U(); // 矩阵U
	Vector s = svd.s(); // 奇异值
	Matrix V = svd.V(); // 矩阵V
	System.out.println(s);
	System.out.println("-------------------");
	System.out.println(V);
	Matrix pc = mat.computePrincipalComponents(3); //将维度降为3
	RowMatrix projected = mat.multiply(pc); //坐标系转换+维度提炼
	System.out.println(projected.numRows());
	System.out.println(projected.numCols());
	
	
	sc.stop();
	sc.close();
    }
}
