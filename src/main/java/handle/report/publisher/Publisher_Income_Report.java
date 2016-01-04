package handle.report.publisher;

import java.io.Serializable;
import java.util.ArrayList;
import handle.report.AbstractHandleReport;
import handle.report.ReportOut;
import scala.Tuple2;
import util.DateUtil;
import util.JsonUtil;
import vo.log1_2.ReqLog1_2;
import vo.report.key.publisher.Publisher_Income_Report_Key;
import vo.report.value.publicReportValue;
import vo.report.value.publisher.Publisher_Income_Report_Value;

public class Publisher_Income_Report extends AbstractHandleReport implements Serializable {

	private static final long serialVersionUID = -5289753523649557692L;

	@Override
	public ArrayList<Tuple2<String, String>> outMap(ReqLog1_2 rl, Object... objects) {

		// 返回集合
		ArrayList<Tuple2<String, String>> re = new ArrayList<Tuple2<String, String>>();
		Publisher_Income_Report_Key key = new Publisher_Income_Report_Key();
		// 日期
		key.setDay(DateUtil.getTimeStampFormat(rl.getTimestamp(), DateUtil.yyyyMMdd));
		// 小时
		key.setHour(Integer.parseInt(DateUtil.getTimeStampFormat(rl.getTimestamp(), DateUtil.HH)));
		// 网站ID
		key.setSite_id(rl.getSiteId());
		// 广告位ID
		key.setSlot_id(rl.getSlotId());
		Publisher_Income_Report_Value value = new Publisher_Income_Report_Value();
		publicReportValue tmp = ReportOut.returnValue(rl);
		value.setAmount(tmp.getAmount());
		re.add(ReportOut.returnReportMapKeyValueInfo(key, value, rl.getPublisherId() + "_Publisher_Income_Report"));
		return re;
	}

	@Override
	public String outReduce(String key, String a0, String a1, Object... objects) throws Exception {

		Publisher_Income_Report_Value prv1 = JsonUtil.toBean(a0, Publisher_Income_Report_Value.class);
		Publisher_Income_Report_Value prv2 = JsonUtil.toBean(a1, Publisher_Income_Report_Value.class);
		Publisher_Income_Report_Value value = new Publisher_Income_Report_Value();
		value.setAmount(prv1.getAmount() + prv2.getAmount());
		return ReportOut.returnReportReduceKeyValueInfo(key, value);
	}

}
