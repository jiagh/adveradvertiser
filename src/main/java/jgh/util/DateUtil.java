package jgh.util;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;

public class DateUtil {

    /**
     * 时间戳转日期字符串
     * 
     * @param timestamp
     * @return
     */
    public static String timeStampToDate(long timestamp) {
	Timestamp ts = new Timestamp(timestamp);
	SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	return sdf.format(ts);
    }

    public static String getDayFormat(long timestamp) {
	SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd-HH");
	return sdf.format(timestamp);
    }

    /**
     * 时间戳转小时
     * 
     * @param timestamp
     * @return
     */
    public static String getDayHour(long timestamp) {
	SimpleDateFormat sdf = new SimpleDateFormat("HH");
	return sdf.format(timestamp);
    }

    public static ArrayList<String> getDateList(String startTime, String endTime) {
	int startValue = Integer.parseInt(startTime.replaceAll("-", ""));
	int end = Integer.parseInt(endTime.replaceAll("-", ""));
	ArrayList<String> timeList = new ArrayList<String>();
	for (int i = startValue; i <= end; i++) {
	    String time = String.valueOf(i);
	    String year = time.substring(0, 4);
	    String month = time.substring(4, 6);
	    String day = time.substring(6, 8);
	    timeList.add(year + "-" + month + "-" + day);
	}
	return timeList;
    }

    /**
     * 时间戳转10分钟区间值
     * 
     * @param timestamp
     * @return
     */
    public static String getTimeMinute(long timestamp) {
	SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm");
	String minute = sdf.format(timestamp);
	minute = minute.substring(0, minute.length() - 1) + "0";
	return minute;
    }

    /**
     * 获取时间段集合内最小值
     * @param tenMap
     * @param startTime
     * @param lowCount
     * @return
     */
    public static String getMinOfMap(HashMap<String, Integer> tenMap, String startTime, int lowCount) {
	String minTime = "";
	for (String s : tenMap.keySet()) {
	    if (Timestamp.valueOf(startTime + ":00").getTime() < Timestamp.valueOf(s + ":00").getTime()) {
		if (tenMap.get(s) <= lowCount) {
		    if (minTime.equals("")) {
			minTime = s;
		    } else {
			minTime = getMin(minTime, s);
		    }
		}
	    }
	}
	if (minTime.equals("")) {
	    minTime = startTime;
	}
	return minTime;
    }

    //字符串时间比较，返回较小值
    public static String getMin(String time1, String time2) {
	if (Timestamp.valueOf(time1 + ":00").getTime() < Timestamp.valueOf(time2 + ":00").getTime()) {
	    return time1;
	} else {
	    return time2;
	}
    }
    
    
    public static HashMap<String,String> getTimeBucket(String time) {
	Long timeStart=Timestamp.valueOf(time + ":00").getTime();
	Long timeEnd=timeStart+600*1000;
	HashMap<String,String> timeMap=new HashMap<String,String>();
	timeMap.put("timeStart", getTimeMinute(timeStart));
	timeMap.put("timeEnd", getTimeMinute(timeEnd));
	return timeMap;
    }

    public static void main(String args[]) {
//	timeStampToDate(Long.parseLong("1449122812709"));
	System.out.println(getTimeBucket("2015-12-03 14:10").toString());
    }
}
