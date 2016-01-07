package jgh.machine;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.recommendation.ALS;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;

public class AlsExample {

    @SuppressWarnings("serial")
    public static void main(String args[]) {
	learnAlsModel();
    }

    public static void learnAlsModel() {
	// init spark
	SparkConf conf = new SparkConf().setAppName("Java Collaborative Filtering Example").setMaster("local[6]");
	JavaSparkContext jsc = new JavaSparkContext(conf);
	//load learn data
	String path = "/jgh/mllib/als/tt.data";
	JavaRDD<String> data = jsc.textFile(path);
	JavaRDD<Rating> ratings = data.map(str -> new Rating(Integer.parseInt(str.split(",")[0]), Integer.parseInt(str.split(",")[1]), Double.parseDouble(str.split(",")[2])));
	MatrixFactorizationModel model = ALS.train(JavaRDD.toRDD(ratings), 10, 10, 0.01);

	//save model to hdfs
	model.save(jsc.sc(), "/jgh/testModel");
	
	//load model in hdfs
	MatrixFactorizationModel sameModel = MatrixFactorizationModel.load(jsc.sc(), "/jgh/testModel");
	//forecast data value
	System.out.println("====:" + sameModel.predict(41, 27));
	jsc.stop();
	jsc.close();

    }
}
