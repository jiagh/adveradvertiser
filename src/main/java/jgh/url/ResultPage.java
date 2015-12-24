package jgh.url;

import java.util.HashMap;

public class ResultPage {

    private String url;
    private String firstTime;
    private HashMap<String,String> maxTimeBucket;
    private int maxcount;
    private HashMap<String,String> firstLowTimeBucket;
    private int threshold;
    public String getUrl() {
        return url;
    }
    public void setUrl(String url) {
        this.url = url;
    }
    public String getFirstTime() {
        return firstTime;
    }
    public void setFirstTime(String firstTime) {
        this.firstTime = firstTime;
    }

    public HashMap<String, String> getMaxTimeBucket() {
        return maxTimeBucket;
    }
    public void setMaxTimeBucket(HashMap<String, String> maxTimeBucket) {
        this.maxTimeBucket = maxTimeBucket;
    }
    public int getMaxcount() {
        return maxcount;
    }
    public void setMaxcount(int maxcount) {
        this.maxcount = maxcount;
    }

    public HashMap<String, String> getFirstLowTimeBucket() {
        return firstLowTimeBucket;
    }
    public void setFirstLowTimeBucket(HashMap<String, String> firstLowTimeBucket) {
        this.firstLowTimeBucket = firstLowTimeBucket;
    }
    public int getThreshold() {
        return threshold;
    }
    public void setThreshold(int threshold) {
        this.threshold = threshold;
    }
}
