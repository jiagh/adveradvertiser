package handle.report.admin;

import java.io.Serializable;
import java.util.ArrayList;

import handle.report.AbstractHandleReport;
import handle.report.ReportOut;
import scala.Tuple2;
import util.DateUtil;
import util.JsonUtil;
import vo.log1_2.ReqLog1_2;
import vo.report.key.admin.Admin_All_Project_Report_Key;
import vo.report.value.publicReportValue;
import vo.report.value.admin.Admin_All_Project_Report_Value;

public class Admin_All_Project_Report extends AbstractHandleReport implements Serializable {

	private static final long serialVersionUID = 6335087267822034232L;

	@Override
	public ArrayList<Tuple2<String, String>> outMap(ReqLog1_2 rl, Object... objects) {

		// 返回集合
		ArrayList<Tuple2<String, String>> re = new ArrayList<Tuple2<String, String>>();

		Admin_All_Project_Report_Key key = new Admin_All_Project_Report_Key();

		// 日期
		key.setDay(DateUtil.getTimeStampFormat(rl.getTimestamp(), DateUtil.yyyyMMdd));
		// 小时
		key.setHour(Integer.parseInt(DateUtil.getTimeStampFormat(rl.getTimestamp(), DateUtil.HH)));

		for (int i = 0; i < rl.getReqs().size(); i++) {

			if (rl.getReqs().get(i).getAdvertisersId() > 0) {

				// 循环分别计算每个广告的
				publicReportValue tmp = ReportOut.returnValue(rl, rl.getReqs().get(i));
				// 广告主ID
				key.setAdvertisers_id(rl.getReqs().get(i).getAdvertisersId());
				// 广告项目ID
				key.setProject_id(rl.getReqs().get(i).getProjectId());
				// 广告活动ID
				key.setCampaign_id(rl.getReqs().get(i).getCampaignId());
				key.setCreative_id(rl.getReqs().get(i).getCreativeId());
				Admin_All_Project_Report_Value value = new Admin_All_Project_Report_Value();
				value.setImp(tmp.getImp());
				value.setClick(tmp.getClick());
				value.setAmount(tmp.getAmount());
				value.setTemplate_req(tmp.getTemplate_req());
				// 输出
				re.add(ReportOut.returnReportMapKeyValueInfo(key, value, "Admin_All_Project_Report"));
			}
		}
		return re;
	}

	@Override
	public String outReduce(String key, String a0, String a1, Object... objects) throws Exception {

		Admin_All_Project_Report_Value prv1 = JsonUtil.toBean(a0, Admin_All_Project_Report_Value.class);
		Admin_All_Project_Report_Value prv2 = JsonUtil.toBean(a1, Admin_All_Project_Report_Value.class);

		// 返回集合
		Admin_All_Project_Report_Value value = new Admin_All_Project_Report_Value();
		value.setClick(prv1.getClick() + prv2.getClick());
		value.setImp(prv1.getImp() + prv2.getImp());
		value.setAmount(prv1.getAmount() + prv2.getAmount());
		value.setTemplate_req(prv1.getTemplate_req() + prv2.getTemplate_req());
		return ReportOut.returnReportReduceKeyValueInfo(key, value);
	}

}
