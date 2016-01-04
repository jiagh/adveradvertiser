package vo.report.key.advertisers;

public class Advertisers_Project_Report_Key {

	// 日期
	private String day;
	// 小时
	private int hour;
	// 广告项目ID
	private int project_id;
	// 广告活动ID
	private int campaign_id;
	// 创意ID
	private int creative_id;

	public int getCreative_id() {
		return creative_id;
	}

	public void setCreative_id(int creative_id) {
		this.creative_id = creative_id;
	}

	public String getDay() {
		return day;
	}

	public void setDay(String day) {
		this.day = day;
	}

	public int getHour() {
		return hour;
	}

	public void setHour(int hour) {
		this.hour = hour;
	}

	public int getProject_id() {
		return project_id;
	}

	public void setProject_id(int project_id) {
		this.project_id = project_id;
	}

	public int getCampaign_id() {
		return campaign_id;
	}

	public void setCampaign_id(int campaign_id) {
		this.campaign_id = campaign_id;
	}

}
