package spark.process;

import java.io.Serializable;
import java.util.ArrayList;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFlatMapFunction;

import handle.report.admin.Admin_All_Advertisers_Cost_Report;
import handle.report.admin.Admin_All_Area_Report;
import handle.report.admin.Admin_All_Day_Report;
import handle.report.admin.Admin_All_Project_Report;
import handle.report.advertisers.Advertisers_Area_Report;
import handle.report.advertisers.Advertisers_Project_Report;
import handle.report.publisher.Publisher_Creative_Report;
import handle.report.publisher.Publisher_Income_Report;
import handle.report.publisher.Publisher_Slot_Report;
import scala.Tuple2;
import util.JsonUtil;
import vo.log1_2.CogtuLog1_2;
import vo.log1_2.ReqLog1_2;

public class ReportKeyValueGenerate implements Serializable {

	private static final long serialVersionUID = -7027388375447881619L;

	public JavaPairRDD<String, String> execute(JavaRDD<String> sourceLog) {

		// 生成报表 KEY VALUE
		JavaPairRDD<String, String> reportKeyValueGenerate = sourceLog.flatMapToPair(new PairFlatMapFunction<String, String, String>() {

			private static final long serialVersionUID = 4300255327160727804L;

			/**
			 * v1.1.0 报表
			 */
			private final Advertisers_Project_Report apr = new Advertisers_Project_Report();
			private final Advertisers_Area_Report aar = new Advertisers_Area_Report();
			private final Admin_All_Project_Report aapr = new Admin_All_Project_Report();
			private final Admin_All_Advertisers_Cost_Report aacr = new Admin_All_Advertisers_Cost_Report();
			private final Admin_All_Day_Report aadr = new Admin_All_Day_Report();
			private final Publisher_Income_Report pir = new Publisher_Income_Report();
			private final Publisher_Slot_Report pslr = new Publisher_Slot_Report();
			private final Publisher_Creative_Report pcr = new Publisher_Creative_Report();

			/**
			 * 1.1.5
			 */

			private final Admin_All_Area_Report aaar = new Admin_All_Area_Report();

			@Override
			public Iterable<Tuple2<String, String>> call(String t) {

				ArrayList<Tuple2<String, String>> re = new ArrayList<Tuple2<String, String>>();

				if (t.indexOf("\"v\":\"1.0\"") != -1 || t.indexOf("\"v\":\"1.1\"") != -1) {

					try {
						// 转成公用BEAN
						CogtuLog1_2 cl = JsonUtil.toBean(t, CogtuLog1_2.class);
						// 判断是否REQLOG
						if (cl instanceof ReqLog1_2) {
							// 转化专属BEAN
							ReqLog1_2 rl = (ReqLog1_2) cl;

							if (rl.getReqType() == ReqLog1_2.REQ_TYPE_AD_IMP || rl.getReqType() == ReqLog1_2.REQ_TYPE_AD_CLICK) {

								// 项目报表
								re.addAll(apr.outMap(rl));
								// 地域报表
								re.addAll(aar.outMap(rl));
								// 管理员广告主报表
								re.addAll(aapr.outMap(rl));
								// 扣费报表
								re.addAll(aacr.outMap(rl));
							}

							// 媒体页面报表 TODO
							// re.addAll(ppr.outMap(rl));
							// 管理员地域报表
							re.addAll(aaar.outMap(rl));
							// 管理员天报表
							re.addAll(aadr.outMap(rl));
							// 网站主收入报表
							re.addAll(pir.outMap(rl));
							// 网站主广告位报表
							re.addAll(pslr.outMap(rl));
							// 网站主创意展示报表
							re.addAll(pcr.outMap(rl));
							// 返回
							return re;
						}

					} catch (Exception e) {
						e.printStackTrace();
						System.out.println("Err Log -> " + t);
					}
				}
				return new ArrayList<Tuple2<String, String>>();
			}
		});

		return reportKeyValueGenerate;
	}
}
