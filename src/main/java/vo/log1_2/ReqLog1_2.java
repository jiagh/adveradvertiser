package vo.log1_2;

import java.util.ArrayList;
import java.util.List;

/*
 * Request Log
 */
public class ReqLog1_2 extends CogtuLog1_2 {

	// 1:广告请求
	public static final int REQ_TYPE_AD_REQ = 1;
	// 2:广告展示
	public static final int REQ_TYPE_AD_IMP = 2;
	// 3:广告点击
	public static final int REQ_TYPE_AD_CLICK = 3;
	// 4:模板请求
	public static final int REQ_TYPE_AD_TEMPLATE = 4;
	// -1:问题日志
	public static final int REQ_TYPE_AD_TROUBLE = -1;

	private Integer reqType;
	//
	private List<AdLog1_2> reqs = new ArrayList<AdLog1_2>();

	public List<AdLog1_2> getReqs() {
		return reqs;
	}

	public void setReqs(List<AdLog1_2> reqs) {
		this.reqs = reqs;
	}

	public Integer getReqType() {
		return reqType;
	}

	public void setReqType(Integer reqType) {
		this.reqType = reqType;
	}
	
	
}