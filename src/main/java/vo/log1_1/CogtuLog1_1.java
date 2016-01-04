package vo.log1_1;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "source")
@JsonSubTypes({ @Type(value = ReqLog1_1.class, name = "req") })
public class CogtuLog1_1 {
	// IP地址
	private String ip;
	// 时间戳
	private long timestamp;
	// slot ID
	private String slotId;
	// 网站ID
	private Integer siteId;
	// 媒体ID
	private Integer publisherId;;
	// URL
	private String pageUri;
	// 来源页
	private String ref;
	// 国家
	private String country;
	// 省
	private String province;
	// 城市
	private String city;
	// 地域编码
	private String addrCode;
	// 浏览器
	private String browser;
	// 操作系统
	private String os;
	// 用户ID
	private String uid;
	// 运行时间
	private long runTime;
	// 当前扣费比例
	private int vBalanceCostPrecent;
	// 当前日志的版本号
	private String v;
	// 操作会话id
	private String sessionId;
	// 日志来源标识
	private String source;
	// agent信息
	private String agent;

	public String getAgent() {
		return agent;
	}

	public void setAgent(String agent) {
		this.agent = agent;
	}

	public String getSource() {
		return source;
	}

	public void setSource(String source) {
		this.source = source;
	}

	public String getSessionId() {
		return sessionId;
	}

	public void setSessionId(String sessionId) {
		this.sessionId = sessionId;
	}

	public String getV() {
		return v;
	}

	public void setV(String v) {
		this.v = v;
	}

	public String getIp() {
		return ip;
	}

	public void setIp(String ip) {
		this.ip = ip;
	}

	public String getSlotId() {
		return slotId;
	}

	public void setSlotId(String slotId) {
		this.slotId = slotId;
	}

	public String getPageUri() {
		return pageUri;
	}

	public void setPageUri(String pageUri) {
		this.pageUri = pageUri;
	}

	public String getRef() {
		return ref;
	}

	public void setRef(String ref) {
		this.ref = ref;
	}

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

	public String getAddrCode() {
		return addrCode;
	}

	public void setAddrCode(String addrCode) {
		this.addrCode = addrCode;
	}

	public String getBrowser() {
		return browser;
	}

	public void setBrowser(String browser) {
		this.browser = browser;
	}

	public String getOs() {
		return os;
	}

	public void setOs(String os) {
		this.os = os;
	}

	public String getUid() {
		return uid;
	}

	public void setUid(String uid) {
		this.uid = uid;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}

	public long getRunTime() {
		return runTime;
	}

	public void setRunTime(long runTime) {
		this.runTime = runTime;
	}

	public Integer getSiteId() {
		return siteId;
	}

	public void setSiteId(Integer siteId) {
		this.siteId = siteId;
	}

	public Integer getPublisherId() {
		return publisherId;
	}

	public void setPublisherId(Integer publisherId) {
		this.publisherId = publisherId;
	}

	public int getvBalanceCostPrecent() {
		return vBalanceCostPrecent;
	}

	public void setvBalanceCostPrecent(int vBalanceCostPrecent) {
		this.vBalanceCostPrecent = vBalanceCostPrecent;
	}

}
