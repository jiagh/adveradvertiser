package handle.report.advertisers;

import java.io.Serializable;
import java.util.ArrayList;

import handle.report.AbstractHandleReport;
import handle.report.ReportOut;
import scala.Tuple2;
import util.DateUtil;
import util.JsonUtil;
import vo.log1_2.ReqLog1_2;
import vo.report.key.advertisers.Advertisers_Project_Report_Key;
import vo.report.value.publicReportValue;
import vo.report.value.advertisers.Advertisers_Project_Report_Value;

public class Advertisers_Project_Report extends AbstractHandleReport implements Serializable {

	private static final long serialVersionUID = -379069469034893417L;

	@Override
	public ArrayList<Tuple2<String, String>> outMap(ReqLog1_2 rl, Object... objects) {

		// 返回集合
		ArrayList<Tuple2<String, String>> re = new ArrayList<Tuple2<String, String>>();
		Advertisers_Project_Report_Key key = new Advertisers_Project_Report_Key();
		// 日期
		key.setDay(DateUtil.getTimeStampFormat(rl.getTimestamp(), DateUtil.yyyyMMdd));
		// 小时
		key.setHour(Integer.parseInt(DateUtil.getTimeStampFormat(rl.getTimestamp(), DateUtil.HH)));

		for (int i = 0; i < rl.getReqs().size(); i++) {

			if (rl.getReqs().get(i).getAdvertisersId() > 0) {

				// VALUE VO
				publicReportValue tmp = ReportOut.returnValue(rl, rl.getReqs().get(i));
				Advertisers_Project_Report_Value value = new Advertisers_Project_Report_Value();
				// 广告项目
				key.setProject_id(rl.getReqs().get(i).getProjectId());
				// 广告活动
				key.setCampaign_id(rl.getReqs().get(i).getCampaignId());
				key.setCreative_id(rl.getReqs().get(i).getCreativeId());
				value.setImp(tmp.getImp());
				value.setClick(tmp.getClick());
				value.setAmount(tmp.getAmount());
				value.setTemplate_req(tmp.getTemplate_req());
				re.add(ReportOut.returnReportMapKeyValueInfo(key, value, rl.getReqs().get(i).getAdvertisersId() + "_Advertisers_Project_Report"));
			}
		}
		return re;
	}

	@Override
	public String outReduce(String key, String a0, String a1, Object... objects) throws Exception {

		Advertisers_Project_Report_Value prv1 = JsonUtil.toBean(a0, Advertisers_Project_Report_Value.class);
		Advertisers_Project_Report_Value prv2 = JsonUtil.toBean(a1, Advertisers_Project_Report_Value.class);
		// 返回集合
		Advertisers_Project_Report_Value value = new Advertisers_Project_Report_Value();
		value.setClick(prv1.getClick() + prv2.getClick());
		value.setImp(prv1.getImp() + prv2.getImp());
		value.setAmount(prv1.getAmount() + prv2.getAmount());
		value.setTemplate_req(prv1.getTemplate_req() + prv2.getTemplate_req());
		return ReportOut.returnReportReduceKeyValueInfo(key, value);
	}

}
