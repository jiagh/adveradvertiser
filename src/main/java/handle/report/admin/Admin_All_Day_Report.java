package handle.report.admin;

import java.io.Serializable;
import java.util.ArrayList;
import handle.report.AbstractHandleReport;
import handle.report.ReportOut;
import scala.Tuple2;
import util.DateUtil;
import util.JsonUtil;
import vo.log1_2.ReqLog1_2;
import vo.report.key.admin.Admin_All_Day_Report_Key;
import vo.report.value.publicReportValue;
import vo.report.value.admin.Admin_All_Day_Report_Value;

public class Admin_All_Day_Report extends AbstractHandleReport implements Serializable {

	private static final long serialVersionUID = -1493406097101269210L;

	@Override
	public ArrayList<Tuple2<String, String>> outMap(ReqLog1_2 rl, Object... objects) {

		// 返回集合
		ArrayList<Tuple2<String, String>> re = new ArrayList<Tuple2<String, String>>();

		Admin_All_Day_Report_Key key = new Admin_All_Day_Report_Key();

		// 日期
		key.setDay(DateUtil.getTimeStampFormat(rl.getTimestamp(), DateUtil.yyyyMMdd));
		// 小时
		key.setHour(Integer.parseInt(DateUtil.getTimeStampFormat(rl.getTimestamp(), DateUtil.HH)));
		//
		publicReportValue tmp = ReportOut.returnValue(rl);
		//
		Admin_All_Day_Report_Value value = new Admin_All_Day_Report_Value();
		//
		value.setFetch_(tmp.getFetch());
		value.setReq(tmp.getReq());
		value.setFilling(tmp.getFilling());
		value.setImp(tmp.getImp());
		value.setClick(tmp.getClick());
		value.setAmount(tmp.getAmount());
		value.setTemplate_req(tmp.getTemplate_req());
		re.add(ReportOut.returnReportMapKeyValueInfo(key, value, "Admin_All_Day_Report"));
		return re;
	}

	@Override
	public String outReduce(String key, String a0, String a1, Object... objects) throws Exception {

		Admin_All_Day_Report_Value prv1 = JsonUtil.toBean(a0, Admin_All_Day_Report_Value.class);
		Admin_All_Day_Report_Value prv2 = JsonUtil.toBean(a1, Admin_All_Day_Report_Value.class);

		// 返回集合
		Admin_All_Day_Report_Value value = new Admin_All_Day_Report_Value();
		value.setFetch_(prv1.getFetch_() + prv2.getFetch_());
		value.setClick(prv1.getClick() + prv2.getClick());
		value.setImp(prv1.getImp() + prv2.getImp());
		value.setAmount(prv1.getAmount() + prv2.getAmount());
		value.setFilling(prv1.getFilling() + prv2.getFilling());
		value.setReq(prv1.getReq() + prv2.getReq());
		value.setTemplate_req(prv1.getTemplate_req() + prv2.getTemplate_req());
		return ReportOut.returnReportReduceKeyValueInfo(key, value);
	}
}
