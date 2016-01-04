package vo.log;

import com.fasterxml.jackson.annotation.JsonProperty;

public class DmpAnalysisLog {

	private String status;

	private int type;

	@JsonProperty("tagset")
	private int tagset;

	@JsonProperty("page_uri")
	private String pageUri;

	// 来源
	private String source = "DmpAnalysisLog";

	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}

	public int getType() {
		return type;
	}

	public void setType(int type) {
		this.type = type;
	}

	public int getTagset() {
		return tagset;
	}

	public void setTagset(int tagset) {
		this.tagset = tagset;
	}

	public String getPageUri() {
		return pageUri;
	}

	public void setPageUri(String pageUri) {
		this.pageUri = pageUri;
	}

	public String getSource() {
		return source;
	}

	public void setSource(String source) {
		this.source = source;
	}

}
