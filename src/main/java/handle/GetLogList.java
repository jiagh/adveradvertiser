package handle;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import offline.OfflineAnalysisRun;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

import util.HiveUtil;
import util.PublicUtil;
import config.Config;

public class GetLogList {

	/**
	 * 通过这个程序把KafkaToHdfs的Log移动到我程序可以读取的地方
	 */
	public static void mvKafkaLog(String inPath, String outPath) throws IOException {

		FileSystem fs = PublicUtil.getFs();
		// 取得目录文件
		Path[] listPaths = FileUtil.stat2Paths(fs.listStatus(new Path(inPath)));
		for (int i = 0; i < listPaths.length; i++) {
			if (fs.exists(new Path(listPaths[i] + "/_SUCCESS"))) {
				Path[] tmp = FileUtil.stat2Paths(fs.listStatus(listPaths[i]));
				for (int j = 0; j < tmp.length; j++) {
					if (tmp[j].toString().indexOf("_SUCCESS") == -1) {
						System.out.println(outPath + "/" + tmp[j].getName() + "-" + listPaths[i].getName().split("-")[1]);
						fs.rename(tmp[j], new Path(outPath + "/" + tmp[j].getName() + "-" + listPaths[i].getName().split("-")[1]));
					}
				}
				// 删除
				fs.delete(listPaths[i], true);
			}
		}
	}

	/**
	 * 取得所有未分析日志
	 */
	public static ArrayList<String> getAllNoAnalysisLog(String path[]) throws InstantiationException, IllegalAccessException, ClassNotFoundException, IOException, SQLException {

		// 返回集合
		ArrayList<String> re = new ArrayList<String>();
		FileSystem fs = PublicUtil.getFs();
		for (int z = 0; z < path.length; z++) {
			FileStatus[] st = fs.listStatus(new Path(path[z]));
			Path[] listPaths = FileUtil.stat2Paths(st);
			Arrays.sort(listPaths);
			for (int i = 0; i < listPaths.length; i++) {
				System.out.println("本次分析日志 = " + listPaths[i].getName());
				// 本次分析的日志数量
				if (i > Config.ANALYSIS_LOG_NUM)
					break;
				else
					re.add(listPaths[i].getName());
			}
		}

		return re;

	}

	public static void main(String[] args) throws ClassNotFoundException, IOException, SQLException {
		insertLogToHive();
	}

	/**
	 * 数据插入到HIVE中
	 */
	public static void insertLogToHive() throws IOException, ClassNotFoundException, SQLException {

		FileSystem fs = PublicUtil.getFs();
		Path[] listPaths = FileUtil.stat2Paths(fs.listStatus(new Path(Config.HDFS_LOG_KAFKA_NORMAL_HIVEFORMAT)));
		for (int i = 0; i < listPaths.length; i++) {
			Path[] tmpPaths = FileUtil.stat2Paths(fs.listStatus(listPaths[i]));

			System.out.println(listPaths[i]);

			if (fs.exists(new Path(listPaths[i] + "/_SUCCESS"))) {

				for (int j = 0; j < tmpPaths.length; j++) {

					System.out.println(tmpPaths[j].getName());

					if (!tmpPaths[j].getName().equals("_SUCCESS")) {

						String datetime = tmpPaths[j].getName().split("-")[1];
						String year = datetime.substring(0, 4);
						String month = datetime.substring(4, 6);
						String day = datetime.substring(6, 8);
						String hour = datetime.substring(8, 10);
						String newName = tmpPaths[j].toString() + "-" + System.currentTimeMillis();
						fs.rename(tmpPaths[j], new Path(newName));
						HiveUtil.executeSql("LOAD DATA INPATH '" + newName + "'  INTO TABLE " + Config.HIVE_COGTU_LOG_SPLIT_TABLE_NAME + " PARTITION(date='" + year + "-" + month + "-" + day + "',hour=" + hour + ")");
					}
				}
			}

			System.out.println("del = " + listPaths[i]);
			fs.delete(listPaths[i], true);
		}
	}

	/**
	 * 本次分析日志移动到临时目录中
	 */
	public static void mvBakAnalysisReqLogs(ArrayList<String> reqLogs) throws IOException {
		FileSystem fs = PublicUtil.getFs();
		fs.mkdirs(new Path(Config.HDFS_ANALYSIS_BATCH_LOG + "/" + OfflineAnalysisRun.analysisFolderName));
		for (int i = 0; i < reqLogs.size(); i++) {
			System.out.println("MV = " + Config.HDFS_LOG_NORMAL + "/" + reqLogs.get(i) + " To = " + Config.HDFS_ANALYSIS_BATCH_LOG + "/" + reqLogs.get(i));
			fs.rename(new Path(Config.HDFS_LOG_NORMAL + "/" + reqLogs.get(i)), new Path(Config.HDFS_ANALYSIS_BATCH_LOG + "/" + OfflineAnalysisRun.analysisFolderName + "/" + reqLogs.get(i)));
		}
	}

	/**
	 * 取得本次需要分析的日志列表
	 */
	public static ArrayList<String> getReqLogInfo() throws InstantiationException, IllegalAccessException, ClassNotFoundException, IOException, SQLException {

		insertLogToHive();
		mvKafkaLog(Config.HDFS_LOG_KAFKA_NORMAL_LOG, Config.HDFS_LOG_NORMAL);
		mvKafkaLog(Config.HDFS_LOGS_KAFKA_ERR_LOG, Config.HDFS_LOG_ERR);
		FileSystem fs = PublicUtil.getFs();
		String path[] = Config.HDFS_LOG_NORMAL.split(",");
		// 得到所有未分析的文件列表 过滤一下文件名把没用的扔掉
		ArrayList<String> fileList = getAllNoAnalysisLog(path);
		// 如果没有需要分析的日志则直接返回
		if (fileList.size() == 0) {
			return fileList;
		}
		// 本次分析的目录
		String analysisFolderName = fileList.get(fileList.size() - 1).split("-")[1] + "-" + System.currentTimeMillis();

		System.out.println("本次分析目录 = " + analysisFolderName);

		// 把本次分析的FTP列表 写入到HDFS
		FSDataOutputStream osList = fs.create(new Path(Config.HDFS_ANALYSIS_WORK + "/" + "analysisFileList"));
		// 循环开始加载文件名并创建线程到线程池中
		for (int j = 0; j < fileList.size(); j++) {
			// 本次分析日志写到HDFS
			osList.writeBytes(fileList.get(j) + Config.NEW_LINE);
		}

		// 刷新缓存
		osList.flush();
		// 关闭
		osList.close();
		// 把本次分析的FTP列表 写入到HDFS
		FSDataOutputStream osFolderName = fs.create(new Path(Config.HDFS_ANALYSIS_WORK + "/" + "analysisFolderName"));
		// 本次分析日志写到HDFS
		osFolderName.writeBytes(analysisFolderName + Config.NEW_LINE);
		// 刷新缓存
		osFolderName.flush();
		// 关闭
		osFolderName.close();
		//
		OfflineAnalysisRun.analysisFolderName = analysisFolderName;
		// 移动原始日志
		mvBakAnalysisReqLogs(fileList);

		return fileList;

	}

}
