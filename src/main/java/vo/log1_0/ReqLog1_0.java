package vo.log1_0;

import java.util.ArrayList;
import java.util.List;

import util.JsonUtil;

/*
 * Request Log
 */
public class ReqLog1_0 extends CogtuLog1_0 {

	public static final int REQ_TYPE_AD_REQ = 1;
	public static final int REQ_TYPE_AD_IMP = 2;
	public static final int REQ_TYPE_AD_CLICK = 3;

	// 1:广告请求 2:广告展示 3：广告点击
	private Integer reqType;
	//
	private List<AdLog1_0> reqs = new ArrayList<AdLog1_0>();

	public List<AdLog1_0> getReqs() {
		return reqs;
	}

	public void setReqs(List<AdLog1_0> reqs) {
		this.reqs = reqs;
	}

	public Integer getReqType() {
		return reqType;
	}

	public void setReqType(Integer reqType) {
		this.reqType = reqType;
	}

}