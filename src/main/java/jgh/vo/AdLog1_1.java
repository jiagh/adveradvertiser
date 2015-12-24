package jgh.vo;

import java.util.Set;

public class AdLog1_1 {

	// 广告位ID
	private String impId;
	// img URL
	private String impUrl;
	// 广告主ID
	private Integer advertisersId = 0;
	// 广告项目ID
	private Integer projectId = 0;
	// 广告计划ID
	private Integer campaignId = 0;
	// 广告ID
	private Integer creativeId = 0;
	// 模板ID
	private Integer templateId = 0;
	// 价格(换算厘)
	private Long price = 0l;
	// 类型 CPC/CPM
	private String priceType;
	// feature列表
	private Set<String> features;
	// tag列表
	private Set<String> tags;

	public String getImpUrl() {
		return impUrl;
	}

	public void setImpUrl(String impUrl) {
		this.impUrl = impUrl;
	}

	public String getImpId() {
		return impId;
	}

	public void setImpId(String impId) {
		this.impId = impId;
	}

	public Integer getAdvertisersId() {
		return advertisersId;
	}

	public void setAdvertisersId(Integer advertisersId) {
		this.advertisersId = advertisersId;
	}

	public Integer getProjectId() {
		return projectId;
	}

	public void setProjectId(Integer projectId) {
		this.projectId = projectId;
	}

	public Integer getCampaignId() {
		return campaignId;
	}

	public void setCampaignId(Integer campaignId) {
		this.campaignId = campaignId;
	}

	public Integer getCreativeId() {
		return creativeId;
	}

	public void setCreativeId(Integer creativeId) {
		this.creativeId = creativeId;
	}

	public Long getPrice() {
		return price;
	}

	public void setPrice(Long price) {
		this.price = price;
	}

	public String getPriceType() {
		return priceType;
	}

	public void setPriceType(String priceType) {
		this.priceType = priceType;
	}

	public Set<String> getFeatures() {
		return features;
	}

	public void setFeatures(Set<String> features) {
		this.features = features;
	}

	public Set<String> getTags() {
		return tags;
	}

	public void setTags(Set<String> tags) {
		this.tags = tags;
	}

	public Integer getTemplateId() {
		return templateId;
	}

	public void setTemplateId(Integer templateId) {
		this.templateId = templateId;
	}
}
