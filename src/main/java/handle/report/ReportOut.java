package handle.report;

import config.Config;
import scala.Tuple2;
import util.JsonUtil;
import vo.log1_2.AdLog1_2;
import vo.log1_2.ReqLog1_2;
import vo.report.value.publicReportValue;

public class ReportOut {

	/**
	 * 合并累加VALUE
	 */
	public static publicReportValue returnValue(ReqLog1_2 rl) {

		publicReportValue prv = new publicReportValue();
		// 接口请求数量
		prv.setFetch(1);

		if (rl.getReqType() == ReqLog1_2.REQ_TYPE_AD_REQ) {
			// 广告位请求量
			prv.setReq(rl.getReqs().size());
			// 填充量
			int filling = 0;
			for (int i = 0; i < rl.getReqs().size(); i++) {
				if (rl.getReqs().get(i).getCampaignType() != null && rl.getReqs().get(i).getCampaignType() != AdLog1_2.CAMPAIGN_TYPE_GROUND) {
					filling++;
				}
			}
			prv.setFilling(filling);

		} else if (rl.getReqType() == ReqLog1_2.REQ_TYPE_AD_IMP) {
			for (int i = 0; i < rl.getReqs().size(); i++) {
				if (rl.getReqs().get(i).getCampaignType() != null && rl.getReqs().get(i).getCampaignType() != AdLog1_2.CAMPAIGN_TYPE_GROUND) {
					// 展示量
					prv.setImp(1 + prv.getImp());
					if (rl.getReqs().get(i).getPriceType().equals("CPM")) {
						prv.setAmount(rl.getReqs().get(i).getPrice() + prv.getAmount());
					}
				}
			}

		} else if (rl.getReqType() == ReqLog1_2.REQ_TYPE_AD_CLICK) {
			for (int i = 0; i < rl.getReqs().size(); i++) {
				if (rl.getReqs().get(i).getCampaignType() != null && rl.getReqs().get(i).getCampaignType() != AdLog1_2.CAMPAIGN_TYPE_GROUND) {
					// 点击量
					prv.setClick(1 + prv.getClick());
					if (rl.getReqs().get(i).getPriceType().equals("CPC")) {
						prv.setAmount(rl.getReqs().get(i).getPrice() + prv.getAmount());
					}
				}
			}
		} else if (rl.getReqType() == ReqLog1_2.REQ_TYPE_AD_TEMPLATE) {
			for (int i = 0; i < rl.getReqs().size(); i++) {
				if (rl.getReqs().get(i).getCampaignType() != null && rl.getReqs().get(i).getCampaignType() != AdLog1_2.CAMPAIGN_TYPE_GROUND) {
					prv.setTemplate_req(1 + prv.getTemplate_req());
				}
			}
		}

		return prv;

	}

	/**
	 * 单独累加VALUE 从AdLog中提取维度的时候会用到
	 */
	public static publicReportValue returnValue(ReqLog1_2 rl, AdLog1_2 al) {

		publicReportValue prv = new publicReportValue();

		if (rl.getReqType() == ReqLog1_2.REQ_TYPE_AD_REQ) {
			// 请求
			prv.setReq(1);
			// 有广告ID则认为有返回广告
			if (al.getCreativeId() > 0) {
				prv.setFilling(1);
			}

		} else if (rl.getReqType() == ReqLog1_2.REQ_TYPE_AD_IMP && al.getCampaignType() != AdLog1_2.CAMPAIGN_TYPE_GROUND) {
			// 展示
			prv.setImp(1);
			// 如果是展示计费则计费
			if (al.getPriceType().equals("CPM")) {
				prv.setAmount(al.getPrice());
			}

		} else if (rl.getReqType() == ReqLog1_2.REQ_TYPE_AD_CLICK && al.getCampaignType() != AdLog1_2.CAMPAIGN_TYPE_GROUND) {
			// 点击
			prv.setClick(1);
			// 如果是点击计费则计费
			if (al.getPriceType().equals("CPC")) {
				prv.setAmount(al.getPrice());
			}
		} else if (rl.getReqType() == ReqLog1_2.REQ_TYPE_AD_TEMPLATE && al.getCampaignType() != AdLog1_2.CAMPAIGN_TYPE_GROUND) {
			// 模板请求
			prv.setTemplate_req(1);
		}

		return prv;
	}

	public static Tuple2<String, String> returnReportMapKeyValueInfo(Object key, Object value, String name) {
		return new Tuple2<String, String>(name + Config.REPORT_KEY_DELIMITED + JsonUtil.toJson(key), name + Config.REPORT_KEY_DELIMITED + JsonUtil.toJson(value));
	}

	public static String returnReportReduceKeyValueInfo(Object key, Object value) {
		return key + Config.REPORT_KEY_DELIMITED + JsonUtil.toJson(value);
	}
}
