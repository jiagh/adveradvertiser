import java.io.Serializable;

public class ManuallyAnalysisVo implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 4077790283234853701L;

	private String date;
	private String hour;
	private String slotId;
	private String uid;
	private int fetch;
	private int imp;
	private int reqImg;
	private int reAd;
	private int click;
	private int reqType;
	private String ip;
	private String tag;
	private String feature;
	private String os;
	private String broser;
	private int creativeId;
	private String ua;
	private String pageUri;
	private String pageUriSplitParameter;
	private long timestamp;

	private String country;

	private String province;
	private String city;

	public String getCountry() {
		return country;
	}

	public void setCountry(String country) {
		this.country = country;
	}

	public String getProvince() {
		return province;
	}

	public void setProvince(String province) {
		this.province = province;
	}

	public String getCity() {
		return city;
	}

	public void setCity(String city) {
		this.city = city;
	}

	public int getReqType() {
		return reqType;
	}

	public void setReqType(int reqType) {
		this.reqType = reqType;
	}

	public int getReAd() {
		return reAd;
	}

	public void setReAd(int reAd) {

		this.reAd = reAd;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}

	public String getHour() {
		return hour;
	}

	public void setHour(String hour) {
		this.hour = hour;
	}

	public String getPageUriSplitParameter() {
		return pageUriSplitParameter;
	}

	public void setPageUriSplitParameter(String pageUriSplitParameter) {
		this.pageUriSplitParameter = pageUriSplitParameter;
	}

	public String getPageUri() {
		return pageUri;
	}

	public void setPageUri(String pageUri) {
		this.pageUri = pageUri;
	}

	public String getUa() {
		return ua;
	}

	public void setUa(String ua) {
		this.ua = ua;
	}

	public int getCreativeId() {
		return creativeId;
	}

	public void setCreativeId(int creativeId) {
		this.creativeId = creativeId;
	}

	public String getOs() {
		return os;
	}

	public void setOs(String os) {
		this.os = os;
	}

	public String getBroser() {
		return broser;
	}

	public void setBroser(String broser) {
		this.broser = broser;
	}

	public int getReqImg() {
		return reqImg;
	}

	public void setReqImg(int reqImg) {
		this.reqImg = reqImg;
	}

	public String getIp() {
		return ip;
	}

	public void setIp(String ip) {
		this.ip = ip;
	}

	public String getTag() {
		return tag;
	}

	public void setTag(String tag) {
		this.tag = tag;
	}

	public String getFeature() {
		return feature;
	}

	public void setFeature(String feature) {
		this.feature = feature;
	}

	public int getFetch() {
		return fetch;
	}

	public void setFetch(int fetch) {
		this.fetch = fetch;
	}

	public int getClick() {
		return click;
	}

	public void setClick(int click) {
		this.click = click;
	}

	public int getImp() {
		return imp;
	}

	public void setImp(int imp) {
		this.imp = imp;
	}

	public String getDate() {
		return date;
	}

	public void setDate(String date) {
		this.date = date;
	}

	public String getSlotId() {
		return slotId;
	}

	public void setSlotId(String slotId) {
		this.slotId = slotId;
	}

	public String getUid() {
		return uid;
	}

	public void setUid(String uid) {
		this.uid = uid;
	}

}
