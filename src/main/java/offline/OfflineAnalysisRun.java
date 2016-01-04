package offline;

import java.io.IOException;
import java.io.Serializable;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.ArrayList;

import config.Config;
import handle.CheckAndCreateTableInfo;
import handle.Complete;
import handle.GetLogList;
import spark.SparkAnalysis;
import util.PublicUtil;
import util.SystemLogUtil;

public class OfflineAnalysisRun implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 5082945881402678325L;
	// 本次分析文件夹名称
	public static String analysisFolderName = "";
	// 本次分析需要的日志
	public static ArrayList<String> reqLogs = null;
	// 本次分析需要的日志
	public static ArrayList<String> trackLogs = null;

	/**
	 * 正常执行分析
	 */
	public void go() throws IOException, InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException, InterruptedException, URISyntaxException, ParseException {

		// 判断是否正在运行
		if (!SystemLogUtil.isStartFile()) {

			// 创建一个开始标识
			SystemLogUtil.createStartFile();
			// 在HDFS上创建一些必要的文件夹
			PublicUtil.mkdirHDFSFolder();

			// 取得本次需要分析的日志
			reqLogs = GetLogList.getReqLogInfo();
			// 日志来源目录
			String in = Config.HDFS_ANALYSIS_BATCH_LOG + "/" + analysisFolderName;
			// 本次临时分析结果数据目录
			String outTmpData = Config.HDFS_ANALYSIS_WORK + "/" + analysisFolderName + "/reportData";
			// 入库语句输出目录
			String outSql = Config.HDFS_ANALYSIS_WORK + "/" + analysisFolderName + "/sql";

			// 如果有日志则开始分析
			if (reqLogs.size() > 0) {
				// 检查数据库结构
				new CheckAndCreateTableInfo();
				// 开始分析
				new SparkAnalysis(in, outTmpData, outSql);
				// 完成操作
				new Complete();
			} else {
				SystemLogUtil.WriteLog("no logs can't analysis", SystemLogUtil.INFO, "");
			}
			// 删除PID文件
			SystemLogUtil.deleteStartFile();

		} else
			SystemLogUtil.WriteLog(Config.PID_FILE_PATH + " " + Config.PID_FILE_NAME + " already exists", SystemLogUtil.INFO, "");
	}

	public static void main(String[] args) {
		try {
			new OfflineAnalysisRun().go();
		} catch (Exception e) {
			e.printStackTrace();
			e.printStackTrace(SystemLogUtil.getPrintWrite());
			SystemLogUtil.WriteLog("adnet-da-report Error ", SystemLogUtil.ERROR, "");

		}

	}
}
