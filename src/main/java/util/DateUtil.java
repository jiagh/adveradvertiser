package util;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class DateUtil {

	public static SimpleDateFormat yyyyMMddHHmm = new SimpleDateFormat("yyyyMMddHHmm");
	public static SimpleDateFormat yyyyMMddHHmmss = new SimpleDateFormat("yyyyMMddHHmmss");
	public static SimpleDateFormat yyyyMMddHH = new SimpleDateFormat("yyyyMMddHH");
	public static SimpleDateFormat yyyyMMdd = new SimpleDateFormat("yyyyMMdd");
	public static SimpleDateFormat yyyy_MM_dd = new SimpleDateFormat("yyyy-MM-dd");
	public static SimpleDateFormat HH = new SimpleDateFormat("HH");
	public static SimpleDateFormat yyyy_MM_dd_HH_mm_ss = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	public static String getTimeStampFormat(long time, SimpleDateFormat sf) {
		return sf.format(new Date(time));
	}

	/**
	 * 取得当前时间
	 */
	public static String getNowDate(SimpleDateFormat sf) {
		return sf.format(new Date());
	}

	/**
	 * 当前日期 减N天
	 */
	public static String getNowDateDayLess(String date, int num, SimpleDateFormat sf) throws ParseException {
		Calendar c1 = Calendar.getInstance();
		c1.setTime(sf.parse(date));
		c1.add(Calendar.DATE, -num);
		return sf.format(c1.getTime());
	}

	/**
	 * 当前日期 加N天
	 */
	public static String getNowDateDayPuls(String date, int num, SimpleDateFormat sf) throws ParseException {
		Calendar c1 = Calendar.getInstance();
		c1.setTime(sf.parse(date));
		c1.add(Calendar.DATE, +num);
		return sf.format(c1.getTime());
	}

	/**
	 * 两个日期之间相差的天数
	 */
	public static int getDaySub(String beginDateStr, String endDateStr, SimpleDateFormat sf) {
		int day = 0;
		java.util.Date beginDate;
		java.util.Date endDate;
		try {
			beginDate = sf.parse(beginDateStr);
			endDate = sf.parse(endDateStr);
			day = (int) ((endDate.getTime() - beginDate.getTime()) / (24 * 60 * 60 * 1000));
		} catch (ParseException e) {
			// TODO 自动生成 catch 块
			e.printStackTrace();
		}
		return day;
	}

	/**
	 * 时间戳转日期字符串.
	 */
	public static String timeStampToDate(long timestamp) {
		Timestamp ts = new Timestamp(timestamp);
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		return sdf.format(ts);
	}

}
