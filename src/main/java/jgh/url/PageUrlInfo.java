package jgh.url;

import java.util.HashMap;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import config.Config;
import jgh.output.SparkHiveOutputFormat;
import jgh.util.DateUtil;
import jgh.util.JsonUtil;
import jgh.vo.CogtuLog1_1;
import jgh.vo.ReqLog1_1;
import scala.Tuple2;

public class PageUrlInfo {

    @SuppressWarnings("serial")
    public static void pageUrlDetailInfo(String args[]) {
	SparkConf sparkConf = new SparkConf().setAppName("pageUrlTotal").setMaster("local[12]");
	JavaSparkContext ctx = new JavaSparkContext(sparkConf);
	StringBuilder inputBuilder = new StringBuilder();
	inputBuilder.append(Config.NAMENODE_ADDRESS).append(Config.HDFS_LOG_BAK).append("/").append("date=").append(args[0]).append("/");
	String inputPath = inputBuilder.toString();
	long t = System.currentTimeMillis();
	StringBuilder outputBuilder = new StringBuilder();
	outputBuilder.append(Config.NAMENODE_ADDRESS).append(Config.HDFS_LOG_MERGE_WORK).append("/jgh/").append(args[0]).append("-").append(t);
	final int threshold = Integer.parseInt(args[1]);
	ctx.textFile(inputPath).filter(new Function<String, Boolean>() {
	    @Override
	    public Boolean call(String v1) throws Exception {
		CogtuLog1_1 cl = JsonUtil.toBean(v1, CogtuLog1_1.class);
		return ((ReqLog1_1) cl).getReqType() == 1;
	    }
	}).mapToPair(new PairFunction<String, String, PageBean>() {
	    @Override
	    public Tuple2<String, PageBean> call(String t) throws Exception {
		CogtuLog1_1 cl = JsonUtil.toBean(t, CogtuLog1_1.class);
		PageBean page = new PageBean();
		String timeMinute = DateUtil.getTimeMinute(cl.getTimestamp());
		page.setStartTime(timeMinute);
		page.setMinTime(cl.getTimestamp());
		String url = cl.getPageUri();
		if (url.contains("?")) {
		    url = url.substring(0, url.indexOf("?"));
		}
		page.setPageUrl(url);
		page.setLowCount(threshold);
		page.setTenMinuteCount(1);
		return new Tuple2<String, PageBean>(timeMinute + "," + url, page);
	    }
	}).reduceByKey(new Function2<PageBean, PageBean, PageBean>() {
	    @Override
	    public PageBean call(PageBean v1, PageBean v2) throws Exception {
		PageBean bean = new PageBean();
		bean.setLowCount(v1.getLowCount());
		bean.setTenMinuteCount(v1.getTenMinuteCount() + v2.getTenMinuteCount());
		bean.setPageUrl(v1.getPageUrl());
		bean.setStartTime(v1.getStartTime());
		long minTme = v1.getMinTime();
		if (minTme > v2.getMinTime()) {
		    minTme = v2.getMinTime();
		}
		bean.setMinTime(minTme);
		return bean;
	    }
	}).mapToPair(new PairFunction<Tuple2<String, PageBean>, String, PageBean>() {
	    @Override
	    public Tuple2<String, PageBean> call(Tuple2<String, PageBean> t) throws Exception {
		PageBean bean = t._2();
		bean.setLowCountTime(t._1().split(",")[0]);

		HashMap<String, Integer> tenMap = new HashMap<String, Integer>();
		tenMap.put(bean.getStartTime(), bean.getTenMinuteCount());
		bean.setTenMinuteList(tenMap);
		return new Tuple2<String, PageBean>(t._1().split(",")[1], bean);
	    }
	}).reduceByKey(new Function2<PageBean, PageBean, PageBean>() {
	    @Override
	    public PageBean call(PageBean v1, PageBean v2) throws Exception {
		HashMap<String, Integer> tenMap = new HashMap<String, Integer>();
		tenMap.putAll(v1.getTenMinuteList());
		tenMap.putAll(v2.getTenMinuteList());
		tenMap.put(v1.getStartTime(), v1.getTenMinuteCount());
		tenMap.put(v2.getStartTime(), v2.getTenMinuteCount());
		long minTime = v1.getMinTime();
		if (v1.getMinTime() > v2.getMinTime()) {
		    minTime = v2.getMinTime();
		}
		if (v1.getTenMinuteCount() > v2.getTenMinuteCount()) {
		    v1.setMinTime(minTime);
		    v1.setTenMinuteList(tenMap);
		    return v1;
		} else {
		    v2.setMinTime(minTime);
		    v2.setTenMinuteList(tenMap);
		    return v2;
		}
	    }
	}).mapToPair(new PairFunction<Tuple2<String, PageBean>, String, String>() {

	    @Override
	    public Tuple2<String, String> call(Tuple2<String, PageBean> t) throws Exception {
		PageBean bean = t._2();
		String lowTime = DateUtil.getMinOfMap(bean.getTenMinuteList(), bean.getStartTime(), bean.getLowCount());
		ResultPage rp = new ResultPage();
		rp.setUrl(bean.getPageUrl());
		rp.setFirstTime(DateUtil.timeStampToDate(bean.getMinTime()));
		rp.setMaxcount(bean.getTenMinuteCount());
		rp.setMaxTimeBucket(DateUtil.getTimeBucket(bean.getStartTime()));
		rp.setThreshold(bean.getLowCount());
		rp.setFirstLowTimeBucket(DateUtil.getTimeBucket(lowTime));
		return new Tuple2<String, String>("", JsonUtil.toJson(rp));
	    }
	}).repartition(2).saveAsHadoopFile(outputBuilder.toString(), String.class, String.class, SparkHiveOutputFormat.class);
	ctx.stop();
	ctx.close();
    }

    public static void main(String args[]) {
	pageUrlDetailInfo(args);
    }

}
