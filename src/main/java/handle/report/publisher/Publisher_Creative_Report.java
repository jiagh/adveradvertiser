package handle.report.publisher;

import java.io.Serializable;
import java.util.ArrayList;
import handle.report.AbstractHandleReport;
import handle.report.ReportOut;
import scala.Tuple2;
import util.DateUtil;
import util.JsonUtil;
import vo.log1_2.ReqLog1_2;
import vo.report.key.publisher.Publisher_Creative_Report_Key;
import vo.report.value.publicReportValue;
import vo.report.value.publisher.Publisher_Creative_Report_Value;

public class Publisher_Creative_Report extends AbstractHandleReport implements Serializable {

	private static final long serialVersionUID = -546893315344610457L;

	@Override
	public ArrayList<Tuple2<String, String>> outMap(ReqLog1_2 rl, Object... objects) {

		// 返回集合
		ArrayList<Tuple2<String, String>> re = new ArrayList<Tuple2<String, String>>();
		Publisher_Creative_Report_Key key = new Publisher_Creative_Report_Key();
		// 日期
		key.setDay(DateUtil.getTimeStampFormat(rl.getTimestamp(), DateUtil.yyyyMMdd));
		// 小时
		key.setHour(Integer.parseInt(DateUtil.getTimeStampFormat(rl.getTimestamp(), DateUtil.HH)));
		// 网站ID
		key.setSite_id(rl.getSiteId());
		// 广告位ID
		key.setSlot_id(rl.getSlotId());

		for (int i = 0; i < rl.getReqs().size(); i++) {

			if (rl.getReqs().get(i).getAdvertisersId() > 0) {
				// VALUE VO
				publicReportValue tmp = ReportOut.returnValue(rl, rl.getReqs().get(i));
				Publisher_Creative_Report_Value value = new Publisher_Creative_Report_Value();
				key.setCreative_id(rl.getReqs().get(i).getCreativeId());
				value.setAmount(tmp.getAmount());
				value.setClick(tmp.getClick());
				value.setImp(tmp.getImp());
				value.setTemplate_req(tmp.getTemplate_req());
				re.add(ReportOut.returnReportMapKeyValueInfo(key, value, rl.getPublisherId() + "_Publisher_Creative_Report"));
			}
		}
		return re;
	}

	@Override
	public String outReduce(String key, String a0, String a1, Object... objects) throws Exception {

		Publisher_Creative_Report_Value prv1 = JsonUtil.toBean(a0, Publisher_Creative_Report_Value.class);
		Publisher_Creative_Report_Value prv2 = JsonUtil.toBean(a1, Publisher_Creative_Report_Value.class);
		// 返回集合
		Publisher_Creative_Report_Value value = new Publisher_Creative_Report_Value();
		value.setClick(prv1.getClick() + prv2.getClick());
		value.setImp(prv1.getImp() + prv2.getImp());
		value.setAmount(prv1.getAmount() + prv2.getAmount());
		value.setTemplate_req(prv1.getTemplate_req() + prv2.getTemplate_req());
		return ReportOut.returnReportReduceKeyValueInfo(key, value);
	}

}
