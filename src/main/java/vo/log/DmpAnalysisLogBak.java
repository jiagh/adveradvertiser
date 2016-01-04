package vo.log;

import java.util.Set;

import com.fasterxml.jackson.annotation.JsonProperty;

public class DmpAnalysisLogBak {
	// 页面URL
	@JsonProperty("page_url")
	private String pageUrl;

	// 标签集ID
	@JsonProperty("tagset_id")
	private int tagsetId;

	// 标签集版本
	@JsonProperty("tagset_ver")
	private int tagsetVer;

	// continerId
	@JsonProperty("container_id")
	private String containerId;

	// 图片URL
	@JsonProperty("img_urls")
	private Set<String> imgUrls;

	// 来源
	private String source = "DmpAnalysisLog";

	// 时间戳
	private long timestamp = System.currentTimeMillis();

	public String getPageUrl() {
		return pageUrl;
	}

	public void setPageUrl(String pageUrl) {
		this.pageUrl = pageUrl;
	}

	public int getTagsetId() {
		return tagsetId;
	}

	public void setTagsetId(int tagsetId) {
		this.tagsetId = tagsetId;
	}

	public int getTagsetVer() {
		return tagsetVer;
	}

	public void setTagsetVer(int tagsetVer) {
		this.tagsetVer = tagsetVer;
	}

	public String getContainerId() {
		return containerId;
	}

	public void setContainerId(String containerId) {
		this.containerId = containerId;
	}

	public Set<String> getImgUrls() {
		return imgUrls;
	}

	public void setImgUrls(Set<String> imgUrls) {
		this.imgUrls = imgUrls;
	}

	public String getSource() {
		return source;
	}

	public void setSource(String source) {
		this.source = source;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}

}
