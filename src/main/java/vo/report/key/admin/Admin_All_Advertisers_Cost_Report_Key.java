package vo.report.key.admin;

public class Admin_All_Advertisers_Cost_Report_Key {

	// 日期
	private String day;
	// 小时
	private int hour;
	// 广告主ID
	private int advertisers_id;
	// 广告项目ID
	private int project_id;
	// 广告活动ID
	private int campaign_id;

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

	public int getAdvertisers_id() {
		return advertisers_id;
	}

	public void setAdvertisers_id(int advertisers_id) {
		this.advertisers_id = advertisers_id;
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
