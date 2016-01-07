package jgh.machine;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.mllib.classification.SVMModel;
import org.apache.spark.mllib.classification.SVMWithSGD;
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.util.MLUtils;

import scala.Tuple2;

public class SVMExample {
    public static void main(String[] args) {
	loadSvmClassfication();
    }

    public static void createSvmClassification() {
	SparkConf conf = new SparkConf().setAppName("SVM Classifier Example").setMaster("local[6]");
	SparkContext sc = new SparkContext(conf);
	String path = "/jgh/mllib/sample_libsvm_data.txt";
	JavaRDD<LabeledPoint> data = MLUtils.loadLibSVMFile(sc, path).toJavaRDD();

	// Split initial RDD into two... [60% training data, 40% testing data].
	JavaRDD<LabeledPoint> training = data.sample(false, 0.6, 11L);
	training.cache();
	JavaRDD<LabeledPoint> test = data.subtract(training);

	// Run training algorithm to build the model.
	int numIterations = 100;
	final SVMModel model = SVMWithSGD.train(training.rdd(), numIterations);

	// Clear the default threshold.
	model.clearThreshold();

	// Compute raw scores on the test set.
	JavaRDD<Tuple2<Object, Object>> scoreAndLabels = test.map(p -> new Tuple2<Object, Object>(model.predict(p.features()),  p.label()));

	// Get evaluation metrics.
	BinaryClassificationMetrics metrics = new BinaryClassificationMetrics(JavaRDD.toRDD(scoreAndLabels));
	
	double auROC = metrics.areaUnderROC();
	System.out.println("Area under ROC = " + auROC);

	// Save and load model
	model.save(sc, "/jgh/svmModel");
	sc.stop();
    }

    @SuppressWarnings({ "resource", "serial" })
    public static void loadSvmClassfication() {
	SparkConf conf = new SparkConf().setAppName("SVM Classifier Example").setMaster("local[6]");
	SparkContext sc = new SparkContext(conf);
	SVMModel sameModel = SVMModel.load(sc, "/jgh/svmModel");
	
	String path = "/jgh/mllib/svmtest.txt";
	JavaRDD<Vector> data = MLUtils.loadVectors(sc, path).toJavaRDD();
	sameModel.predict(data).foreach(new VoidFunction<Double>() {
	    @Override
	    public void call(Double t) throws Exception {
		System.out.println("=============>"+t);
		
	    }
	});
	
	
	sc.stop();
    }

}
