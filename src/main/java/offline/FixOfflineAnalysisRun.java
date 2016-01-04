package offline;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

import config.Config;
import handle.DeleteMysqlReportData;
import util.MysqlUtil;

public class FixOfflineAnalysisRun {

	private static int threadNum = 0;
	private static int threadNumMax = 20;

	public static synchronized void control(String Symbol) {
		if (Symbol.equals("+")) {
			threadNum++;
		} else if (Symbol.equals("-")) {
			threadNum--;
		}
	}

	/**
	 * 删除数据库数据
	 */
	public void DeleteMysql(String[] args)

	throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException, InterruptedException {

		MysqlUtil mt = new MysqlUtil();

		ArrayList<HashMap<String, Object>> dbInfo = mt.selectSql("SELECT ip,port,username,password,dbname FROM user_db_location GROUP BY ip,port,dbname");

		for (int i = 0; i < dbInfo.size(); i++) {

			//
			MysqlUtil tmp = new MysqlUtil();
			// 连接数据库
			tmp.mysqlConn(dbInfo.get(i).get("ip").toString(), dbInfo.get(i).get("port").toString(), dbInfo.get(i).get("username").toString(), dbInfo.get(i).get("password").toString(), dbInfo.get(i).get("dbname").toString());
			// 所有表
			ArrayList<HashMap<String, Object>> tables = tmp.selectSql("SELECT table_name from information_schema.tables WHERE table_schema='" + dbInfo.get(i).get("dbname").toString() + "'");

			for (int j = 0; j < tables.size(); j++) {

				String tableName = tables.get(j).get("table_name").toString();

				if (threadNum > threadNumMax) {
					Thread.sleep(5000);
				}

				if (tableName.indexOf("_report") != -1) {
					DeleteMysqlReportData dt = new DeleteMysqlReportData();
					dt.setMt(tmp);
					dt.setDelSql("DELETE FROM " + tableName + " WHERE " + args[0] + " = '" + args[1] + "'");
					control("+");
					dt.start();
				}
			}

			while (threadNum > 0) {
				Thread.sleep(5000);
			}
		}
	}

	/**
	 * 取出相应的日志
	 */
	public void reMvLog(String[] args) throws IOException {

		// 创建HDFS连接
		FileSystem fs = FileSystem.get(URI.create(Config.NAMENODE_ADDRESS), new Configuration());
		// 整天操作
		if (args[0].equals("day")) {
			args[1] = args[1].replaceAll("-", "");
			String yyyy = args[1].substring(0, 4);
			String mm = args[1].substring(4, 6);
			String dd = args[1].substring(6, 8);
			// 取得目录文件
			Path[] listPaths = FileUtil.stat2Paths(fs.listStatus(new Path(Config.HIVE_COGTU_LOG_TABLE_PATH + "/date=" + yyyy + "-" + mm + "-" + dd)));
			// 把日志文件全部MV到HDFS_LOGS_REQ
			for (int i = 0; i < listPaths.length; i++) {
				System.out.println(listPaths[i] + " To " + Config.HDFS_LOG_NORMAL + "/" + listPaths[i].getName());
				fs.rename(listPaths[i], new Path(Config.HDFS_LOG_NORMAL + "/" + listPaths[i].getName()));
			}
		}
		// 单次分析操作
		else if (args[0].equals("complete_folder_name")) {

			// 取得目录文件
			Path[] listPaths = FileUtil.stat2Paths(fs.listStatus(new Path(Config.HDFS_ANALYSIS_BATCH_LOG + "/" + args[1] + "/")));
			// 把日志文件全部MV到HDFS_LOGS_REQ
			for (int i = 0; i < listPaths.length; i++) {
				fs.rename(listPaths[i], new Path(Config.HDFS_LOG_NORMAL + "/" + listPaths[i].getName()));
			}
		}
	}

	public static void main(String[] args) throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException, InterruptedException, IOException, URISyntaxException, ParseException {

		if (args.length < 3) {
			System.err.println("Usage: FixOfflineAnalysisRun <day or complete_folder_name> <value> <isRun>");
			System.exit(1);
		}

		FixOfflineAnalysisRun foa = new FixOfflineAnalysisRun();
		foa.DeleteMysql(args);
		foa.reMvLog(args);

		if (args[2].equals("true"))
			new OfflineAnalysisRun().go();
	}
}
