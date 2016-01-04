package handle.report.admin;

import java.io.Serializable;
import java.util.ArrayList;
import handle.report.AbstractHandleReport;
import handle.report.ReportOut;
import scala.Tuple2;
import util.DateUtil;
import util.JsonUtil;
import vo.log1_2.ReqLog1_2;
import vo.report.key.admin.Admin_All_Advertisers_Cost_Report_Key;
import vo.report.value.admin.Admin_All_Advertisers_Cost_Report_Value;

public class Admin_All_Advertisers_Cost_Report extends AbstractHandleReport implements Serializable {

	private static final long serialVersionUID = -8638073557923675769L;

	@Override
	public ArrayList<Tuple2<String, String>> outMap(ReqLog1_2 rl, Object... objects) {

		// 返回集合
		ArrayList<Tuple2<String, String>> re = new ArrayList<Tuple2<String, String>>();

		Admin_All_Advertisers_Cost_Report_Key key = new Admin_All_Advertisers_Cost_Report_Key();

		// 日期
		key.setDay(DateUtil.getTimeStampFormat(rl.getTimestamp(), DateUtil.yyyyMMdd));
		// 小时
		key.setHour(Integer.parseInt(DateUtil.getTimeStampFormat(rl.getTimestamp(), DateUtil.HH)));

		for (int i = 0; i < rl.getReqs().size(); i++) {

			if (rl.getReqs().get(i).getAdvertisersId() > 0) {

				// VALUE VO
				Admin_All_Advertisers_Cost_Report_Value value = new Admin_All_Advertisers_Cost_Report_Value();

				// 广告主ID
				key.setAdvertisers_id(rl.getReqs().get(i).getAdvertisersId());
				// 广告项目ID
				key.setProject_id(rl.getReqs().get(i).getProjectId());
				// 广告活动ID
				key.setCampaign_id(rl.getReqs().get(i).getCampaignId());

				if ((rl.getReqType() == ReqLog1_2.REQ_TYPE_AD_IMP && rl.getReqs().get(i).getPriceType().equals("CPM")) || (rl.getReqType() == ReqLog1_2.REQ_TYPE_AD_CLICK && rl.getReqs().get(i).getPriceType().equals("CPC"))) {

					int coupon = (int) (rl.getReqs().get(i).getPrice() * ((double) rl.getvBalanceCostPrecent() / 100));

					// 现金扣费
					value.setAmount(rl.getReqs().get(i).getPrice() - coupon);
					// 优惠券扣费
					value.setV_amount(coupon);
				}

				// 输出
				re.add(ReportOut.returnReportMapKeyValueInfo(key, value, "Admin_All_Advertisers_Cost_Report"));

			}

		}

		return re;
	}

	@Override
	public String outReduce(String key, String a0, String a1, Object... objects) throws Exception {

		Admin_All_Advertisers_Cost_Report_Value prv1 = JsonUtil.toBean(a0, Admin_All_Advertisers_Cost_Report_Value.class);
		Admin_All_Advertisers_Cost_Report_Value prv2 = JsonUtil.toBean(a1, Admin_All_Advertisers_Cost_Report_Value.class);

		// 返回集合
		Admin_All_Advertisers_Cost_Report_Value value = new Admin_All_Advertisers_Cost_Report_Value();
		value.setAmount(prv1.getAmount() + prv2.getAmount());
		value.setV_amount(prv1.getV_amount() + prv2.getV_amount());
		return ReportOut.returnReportReduceKeyValueInfo(key, value);
	}
}
