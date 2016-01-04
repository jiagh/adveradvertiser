package handle;

import java.io.IOException;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

import config.Config;
import offline.OfflineAnalysisRun;
import spark.MergeLog;
import util.DateUtil;
import util.MysqlUtil;
import util.PublicUtil;

public class Complete {

	/**
	 * 完成分析后的操作
	 */
	public Complete() throws ClassNotFoundException, SQLException, IOException, InterruptedException, ParseException, InstantiationException, IllegalAccessException {

		logToHive();
		updateAllAdvertisersCost();
		if (DateUtil.getNowDate(DateUtil.HH).equals("01") || DateUtil.getNowDate(DateUtil.HH).equals("02") || DateUtil.getNowDate(DateUtil.HH).equals("03") || DateUtil.getNowDate(DateUtil.HH).equals("04")
				|| DateUtil.getNowDate(DateUtil.HH).equals("05")) {
			new MergeLog().go(DateUtil.getNowDateDayLess(DateUtil.getNowDate(DateUtil.yyyy_MM_dd), 1, DateUtil.yyyy_MM_dd));
			new MergeTable().go(DateUtil.getNowDateDayLess(DateUtil.getNowDate(DateUtil.yyyy_MM_dd), 1, DateUtil.yyyy_MM_dd));
		}

		deleteAll();
	}

	/**
	 * 删除本次分析所产生的目录
	 */
	public static void deleteAll() throws IOException {
		FileSystem fs = PublicUtil.getFs();
		fs.delete(new Path(Config.HDFS_ANALYSIS_WORK + "/" + OfflineAnalysisRun.analysisFolderName), true);
	}

	/**
	 * 更新所有广告主余额
	 */
	public static void updateAllAdvertisersCost() {

		// 连接数据库
		MysqlUtil db = new MysqlUtil();

		ArrayList<HashMap<String, Object>> adv = db.selectSql("SELECT * FROM `" + Config.MYSQL_DBNAME_LOGIC + "`.advertisers");

		for (int i = 0; i < adv.size(); i++) {
			db.insertSql("UPDATE `" + Config.MYSQL_DBNAME_LOGIC + "`.advertisers SET total_cost = (SELECT IFNULL(SUM(amount),0) FROM `" + Config.MYSQL_DBNAME_BASIC_ANALYSIS + "`.admin_all_advertisers_cost_report WHERE advertisers_id =  "
					+ adv.get(i).get("advertisers_id") + ") WHERE advertisers_id = " + adv.get(i).get("advertisers_id"));
			db.insertSql("UPDATE `" + Config.MYSQL_DBNAME_LOGIC + "`.advertisers SET total_v_cost = (SELECT IFNULL(SUM(v_amount),0) FROM `" + Config.MYSQL_DBNAME_BASIC_ANALYSIS + "`.admin_all_advertisers_cost_report WHERE advertisers_id = "
					+ adv.get(i).get("advertisers_id") + ") WHERE advertisers_id = " + adv.get(i).get("advertisers_id"));
		}
	}

	/**
	 * 按年月日文件夹备份日志
	 */
	public static void logToHive() throws SQLException, ClassNotFoundException, IOException, InterruptedException {

		FileSystem fs = PublicUtil.getFs();
		// 取得目录文件
		Path[] listPaths = FileUtil.stat2Paths(fs.listStatus(new Path(Config.HDFS_ANALYSIS_BATCH_LOG + "/" + OfflineAnalysisRun.analysisFolderName)));

		if (listPaths != null) {

			for (int i = 0; i < listPaths.length; i++) {

				if (listPaths[i].getName().indexOf("click") != -1 || listPaths[i].getName().indexOf("fetch") != -1 || listPaths[i].getName().indexOf("imp") != -1 || listPaths[i].getName().indexOf("trouble") != -1) {

					String year = listPaths[i].getName().split("-")[1].substring(0, 4);
					String month = listPaths[i].getName().split("-")[1].substring(4, 6);
					String day = listPaths[i].getName().split("-")[1].substring(6, 8);

					if (!fs.exists(new Path(Config.HDFS_LOG_BAK + "/" + year + "/"))) {
						fs.mkdirs(new Path(Config.HDFS_LOG_BAK + "/" + year + "/"));
					}
					if (!fs.exists(new Path(Config.HDFS_LOG_BAK + "/" + year + "/" + month + "/"))) {
						fs.mkdirs(new Path(Config.HDFS_LOG_BAK + "/" + year + "/" + month + "/"));
					}
					if (!fs.exists(new Path(Config.HDFS_LOG_BAK + "/" + year + "/" + month + "/" + day + "/"))) {
						fs.mkdirs(new Path(Config.HDFS_LOG_BAK + "/" + year + "/" + month + "/" + day + "/"));
					}

					// 复制到HIVE
					FileUtil.copy(fs, listPaths[i], fs, new Path(Config.HIVE_COGTU_LOG_TABLE_PATH + "/date=" + year + "-" + month + "-" + day + "/" + listPaths[i].getName()), false, new Configuration());
					// 复制
					// FileUtil.copy(fs, listPaths[i], fs, new
					// Path(Config.HDFS_LOG_BAK + "/" + year + "/" + month + "/"
					// + day + "/" + listPaths[i].getName()), false, new
					// Configuration());
				}
			}
		}
	}

	/**
	 * 算法支持数据更新到redis
	 */
	public static void aigorithmSupportDataToRedis() {

	}
}
