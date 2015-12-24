package jgh.url;

import java.util.HashMap;

@SuppressWarnings("serial")
public class PageBean implements java.io.Serializable {
    private String pageUrl;

    private String startTime;
    private int tenMinuteCount;
    private String tenMinue;
    private String lowCountTime;
    private long minTime;
    private int lowCount;
    private HashMap<String,Integer> tenMinuteList=new HashMap<String,Integer>();
    public HashMap<String, Integer> getTenMinuteList() {
        return tenMinuteList;
    }

    public void setTenMinuteList(HashMap<String, Integer> tenMinuteList) {
        this.tenMinuteList = tenMinuteList;
    }

    public String getPageUrl() {
	return pageUrl;
    }

    public void setPageUrl(String pageUrl) {
	this.pageUrl = pageUrl;
    }

    public String getStartTime() {
	return startTime;
    }

    public void setStartTime(String startTime) {
	this.startTime = startTime;
    }

    public int getTenMinuteCount() {
	return tenMinuteCount;
    }

    public void setTenMinuteCount(int tenMinuteCount) {
	this.tenMinuteCount = tenMinuteCount;
    }

    public long getMinTime() {
	return minTime;
    }

    public void setMinTime(long minTime) {
	this.minTime = minTime;
    }

    public int getLowCount() {
	return lowCount;
    }

    public void setLowCount(int lowCount) {
	this.lowCount = lowCount;
    }

    public String getTenMinue() {
	return tenMinue;
    }

    public void setTenMinue(String tenMinue) {
	this.tenMinue = tenMinue;
    }

    public String getLowCountTime() {
	return lowCountTime;
    }

    public void setLowCountTime(String lowCountTime) {
	this.lowCountTime = lowCountTime;
    }

}
