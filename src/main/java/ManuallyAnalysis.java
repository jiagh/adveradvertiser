import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.storage.StorageLevel;

import util.DateUtil;
import util.JsonUtil;
import vo.log1_2.CogtuLog1_2;
import vo.log1_2.ReqLog1_2;

public class ManuallyAnalysis {

	public static void main(String[] args) {

		if (args.length < 4) {
			System.err.println("Usage: ManuallyAnalysis <logPath> <sql> <outName> <partitionNum>");
			System.exit(1);
		}

		SparkConf sparkConf = new SparkConf().setAppName("ManuallyAnalysis");
		SparkContext sc = new SparkContext(sparkConf);
		JavaSparkContext ctx = new JavaSparkContext(sc);
		SQLContext sqlContext = new org.apache.spark.sql.SQLContext(sc);

		JavaRDD<String> sourceLog = null;
		String paths[] = args[0].split(",");
		for (int j = 0; j < paths.length; j++) {
			// 加载数据
			JavaRDD<String> tmp = ctx.textFile(paths[j]).persist(StorageLevel.MEMORY_AND_DISK());
			sourceLog.union(tmp);
		}

		JavaRDD<ManuallyAnalysisVo> step1 = sourceLog.map(new Function<String, ManuallyAnalysisVo>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = -2508008694119782955L;

			@Override
			public ManuallyAnalysisVo call(String v1) throws Exception {
				// 转成公用BEAN
				CogtuLog1_2 cl = JsonUtil.toBean(v1, CogtuLog1_2.class);
				// 判断是否REQLOG
				if (cl instanceof ReqLog1_2) {
					// 转化专属BEAN
					ReqLog1_2 rl = (ReqLog1_2) cl;
					ManuallyAnalysisVo mav = new ManuallyAnalysisVo();
					mav.setDate(DateUtil.getTimeStampFormat(rl.getTimestamp(), DateUtil.yyyy_MM_dd));
					mav.setHour(DateUtil.getTimeStampFormat(rl.getTimestamp(), DateUtil.HH));
					mav.setUid(rl.getUid());
					mav.setSlotId(rl.getSlotId());
					mav.setIp(rl.getIp());
					mav.setBroser(rl.getBrowser());
					mav.setOs(rl.getOs());
					mav.setUa(rl.getAgent());
					mav.setPageUri(cl.getPageUri());
					mav.setPageUriSplitParameter(cl.getPageUri().split("\\?")[0]);
					mav.setTimestamp(rl.getTimestamp());
					mav.setFetch(1);
					mav.setReqType(rl.getReqType());
					mav.setReqImg(rl.getReqs().size());
					mav.setCountry(rl.getCountry());
					mav.setProvince(rl.getProvince());
					mav.setCity(rl.getCity());

					if (rl.getReqType() == 2)
						mav.setImp(1);
					if (rl.getReqType() == 3)
						mav.setClick(1);
					//
					for (int i = 0; i < rl.getReqs().size(); i++) {

						if (rl.getReqs().get(i).getCreativeId() != 0)
							mav.setReAd(1);

						if (rl.getReqs().get(i).getTags() != null)
							mav.setTag(rl.getReqs().get(i).getTags().toString());
						if (rl.getReqs().get(i).getFeatures() != null)
							mav.setFeature(rl.getReqs().get(i).getFeatures().toString());
						if (rl.getReqs().get(i).getCreativeId() != null)
							mav.setCreativeId(rl.getReqs().get(i).getCreativeId());
					}

					return mav;
				}

				return new ManuallyAnalysisVo();
			}
		}).filter(new Function<ManuallyAnalysisVo, Boolean>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = -5001456609233074386L;

			@Override
			public Boolean call(ManuallyAnalysisVo v1) throws Exception {

				if (v1 == null)
					return false;
				else
					return true;
			}
		});

		DataFrame df = sqlContext.createDataFrame(step1, ManuallyAnalysisVo.class);

		sqlContext.registerDataFrameAsTable(df, "df");

		sqlContext.sql(args[1]).toJavaRDD().repartition(Integer.parseInt(args[3])).saveAsTextFile("/manuallyAnalysis/" + System.currentTimeMillis() + "-" + args[2]);

	}
}
