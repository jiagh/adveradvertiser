package spark.process;

import handle.organization.OrganizationSql;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;
import util.JsonUtil;
import util.MysqlUtil;
import util.SystemLogUtil;
import vo.report.key.admin.Admin_All_Advertisers_Cost_Report_Key;
import vo.report.value.admin.Admin_All_Advertisers_Cost_Report_Value;
import config.Config;

public class OrganizaInsertSql implements Serializable {

	private static final long serialVersionUID = -2206292959569744374L;

	public JavaRDD<String> execute(JavaPairRDD<String, Iterable<String>> reportValueMergeGroupBy,
			final Broadcast<String> analysisFolderName, String outSql, final boolean merge, final String mergeDay) {

		/**
		 * 生成语句并插入数据库
		 */
		JavaRDD<String> organizaInsertSql = reportValueMergeGroupBy
				.map(new Function<Tuple2<String, Iterable<String>>, String>() {

					private static final long serialVersionUID = -3085616189443030262L;
					// 检查广告主是否还有虚拟金额的结果
					private HashMap<Integer, Long> overCoupon = getOverCoupon();

					// 取得虚拟账户
					public HashMap<Integer, Long> getOverCoupon() {

						// 取得所有广告主 虚拟账户余额 用于扣费
						MysqlUtil db = new MysqlUtil();
						HashMap<Integer, Long> re = new HashMap<Integer, Long>();

						try {
							ArrayList<HashMap<String, Object>> overCouponAr = db.selectSql(
									"SELECT A.advertisers_id,IFNULL(SUM(C.v_amount),0)-IFNULL(SUM(B.v_amount),0) AS over FROM  `"
											+ Config.MYSQL_DBNAME_LOGIC + "`.recharge AS C RIGHT JOIN `"
											+ Config.MYSQL_DBNAME_LOGIC
											+ "`.advertisers AS A ON A.advertisers_id = C.advertisers_id LEFT JOIN (SELECT A.advertisers_id,SUM(v_amount) AS v_amount FROM `"
											+ Config.MYSQL_DBNAME_BASIC_ANALYSIS
											+ "`.admin_all_advertisers_cost_report AS A GROUP BY A.advertisers_id) AS B ON A.advertisers_id = B.advertisers_id  GROUP BY A.advertisers_id");
							for (int i = 0; i < overCouponAr.size(); i++) {
								if (overCouponAr.get(i).get("over") != null)
									re.put(Integer.parseInt(overCouponAr.get(i).get("advertisers_id").toString()),
											Long.parseLong(overCouponAr.get(i).get("over").toString()));
								else
									re.put(Integer.parseInt(overCouponAr.get(i).get("advertisers_id").toString()), 0l);
							}
						} catch (Exception e) {
							e.printStackTrace();
							SystemLogUtil.WriteLog("adexchange-analysis Error ", SystemLogUtil.ERROR, "");
						}
						return re;
					}

					/**
					 * 检查广告主是否还有虚拟金额
					 */
					public ArrayList<String> checkCost(Iterable<String> values) {

						ArrayList<String> re = new ArrayList<String>();

						Iterator<String> tmpIt = values.iterator();

						while (tmpIt.hasNext()) {

							String value = tmpIt.next().toString();
							String tmp[] = value.toString().split(Config.REPORT_VALUE_DELIMITED);
							Admin_All_Advertisers_Cost_Report_Key aacrk = JsonUtil.toBean(tmp[0],
									Admin_All_Advertisers_Cost_Report_Key.class);
							Admin_All_Advertisers_Cost_Report_Value pr = JsonUtil.toBean(tmp[1],
									Admin_All_Advertisers_Cost_Report_Value.class);

							if (overCoupon.get(aacrk.getAdvertisers_id()) == null)
								re.add(value);
							else {

								// 如果虚拟账户余额不足则不需要在从虚拟账户扣费
								if (overCoupon.size() == 0
										|| overCoupon.get(aacrk.getAdvertisers_id()) - pr.getV_amount() < 0) {
									pr.setAmount(pr.getAmount() + pr.getV_amount());
									pr.setV_amount(0);
									re.add(JsonUtil.toJson(aacrk) + Config.REPORT_VALUE_DELIMITED
											+ JsonUtil.toJson(pr));
								}
								// 如果正常则设置内存中余额
								else {
									overCoupon.put(aacrk.getAdvertisers_id(),
											overCoupon.get(aacrk.getAdvertisers_id()) - pr.getV_amount());
									re.add(value);
								}
							}
						}
						return re;
					}

					@Override
					public String call(Tuple2<String, Iterable<String>> v1) throws Exception {

						// 扣费因为牵扯一致性需要做校验
						if (v1._1().equals("Admin_All_Advertisers_Cost_Report")) {
							return OrganizationSql.organizationSqlReportReduceOut(v1._1(), checkCost(v1._2()),
									analysisFolderName.value());
						} else {
							return OrganizationSql.organizationSqlReportReduceOut(v1._1(), v1._2(),
									analysisFolderName.value());
						}
					}
				});

		// 保存SQL语句
		organizaInsertSql.repartition(Config.ORGANIZA_INSERTSQL_NUM_PARTITIONS).saveAsTextFile(outSql);
		return organizaInsertSql;

	}
}
