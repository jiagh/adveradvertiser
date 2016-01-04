package spark;

import java.io.Serializable;
import offline.OfflineAnalysisRun;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import spark.process.FilterLog;
import spark.process.OrganizaInsertSql;
import spark.process.ReportKeyValueGenerate;
import spark.process.ReportValueMerge;
import util.PublicUtil;

public class SparkAnalysis implements Serializable {

	private static final long serialVersionUID = -7859864458357230746L;

	public SparkAnalysis(String in, String outTmpData, String outSql) {

		JavaSparkContext ctx = PublicUtil.getJavaSparkContext("SparkAnalysisRun");
		// 全局变量
		final Broadcast<String> analysisFolderName = ctx.broadcast(OfflineAnalysisRun.analysisFolderName);
		// 加载数据
		JavaRDD<String> sourceLog = ctx.textFile(in);
		// 过滤日志
		FilterLog fl = new FilterLog();
		JavaRDD<String> log = fl.execute(sourceLog);
		// 生成报表 KEY VALUE
		ReportKeyValueGenerate rkvg = new ReportKeyValueGenerate();
		JavaPairRDD<String, String> reportKeyValueGenerate = rkvg.execute(log);
		// 累计VALUE
		ReportValueMerge rvm = new ReportValueMerge();
		rvm.execute(reportKeyValueGenerate, ctx, outTmpData);
		// 组织入库语句并插入
		OrganizaInsertSql ois = new OrganizaInsertSql();
		ois.execute(rvm.getReportValueMergeGroupBy(), analysisFolderName, outSql, false, null);

		ctx.stop();
		ctx.close();

	}

}
