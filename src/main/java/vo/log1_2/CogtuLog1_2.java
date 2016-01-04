package vo.log1_2;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;

import util.JsonUtil;

import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "source")
@JsonSubTypes({ @Type(value = ReqLog1_2.class, name = "req") })
public class CogtuLog1_2 {
	// IP地址
	private String ip;
	// 时间戳
	private Long timestamp;
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
	private Long runTime;
	// 当前扣费比例
	private Integer vBalanceCostPrecent;
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

	public Long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(Long timestamp) {
		this.timestamp = timestamp;
	}

	public Long getRunTime() {
		return runTime;
	}

	public void setRunTime(Long runTime) {
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

	public Integer getvBalanceCostPrecent() {
		return vBalanceCostPrecent;
	}

	public void setvBalanceCostPrecent(Integer vBalanceCostPrecent) {
		this.vBalanceCostPrecent = vBalanceCostPrecent;
	}

	public static void main(String[] args) {
		String log = "{\"addrCode\":\"\",\"agent\":\"python-requests/2.8.1\",\"browser\":\"Python-requests|2\",\"city\":\"\",\"country\":\"局域网\",\"ip\":\"172.16.1.16\",\"os\":\"unknown\",\"pageUri\":\"http://games.sina.com.cn/ol/n/2015-09-08/fxhqtsx3623702.shtml\",\"province\":\"局域网\",\"publisherId\":2,\"reqType\":1,\"reqs\":[{\"advertisersId\":234,\"campaignId\":338,\"creativeId\":14,\"impId\":\"http://n.sinaimg.cn/transform/20150908/dFYu-fxhqhui4951485.JPG\",\"price\":3000,\"priceType\":\"CPM\",\"projectId\":1200,\"templateId\":14,\"campaignType\":1}],\"runTime\":7,\"sessionId\":\"e7aae22b-fff5-440f-81ba-9294148be6f3\",\"siteId\":3,\"slotId\":\"ct-00000000-0\",\"source\":\"req\",\"timestamp\":1449471025385,\"uid\":\"55f2b772.5170932\",\"v\":\"1.1\",\"vBalanceCostPrecent\":1}";

		ReqLog1_2 rl = (ReqLog1_2) JsonUtil.toBean(log, ReqLog1_2.class);
		
		

	}
}
