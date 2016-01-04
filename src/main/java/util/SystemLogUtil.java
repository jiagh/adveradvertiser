package util;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

import config.Config;

public class SystemLogUtil {

	public static String ERROR = "ERROR";
	public static String INFO = "INFO";

	// 创建一个启动文件
	public static void createStartFile() throws FileNotFoundException {
		WriteLog("adnet-da-report start", SystemLogUtil.INFO, "");
		PrintWriter pw = new PrintWriter(Config.PID_FILE_PATH + "/" + Config.PID_FILE_NAME);
		pw.close();
	}

	// 判断文件是否存在
	public static boolean isStartFile() {
		return new File(Config.PID_FILE_PATH + "/" + Config.PID_FILE_NAME).isFile();
	}

	// 删除一个启动文件
	public static void deleteStartFile() throws FileNotFoundException {
		WriteLog("adnet-da-report end", SystemLogUtil.INFO, "");
		new File(Config.PID_FILE_PATH + "/" + Config.PID_FILE_NAME).delete();
	}

	public static PrintWriter getPrintWrite() {

		FileWriter writer = null;
		try {
			// 打开一个写文件器，构造函数中的第二个参数true表示以追加形式写文件
			writer = new FileWriter(Config.SYSTEM_LOG, true);
			PrintWriter pw = new PrintWriter(writer);
			return pw;
		} catch (IOException e) {
			e.printStackTrace();
		}

		return null;
	}

	public static void WriteLog(String content, String level, String info) {

		String out = DateUtil.getNowDate(DateUtil.yyyyMMddHH) + " - " + level + " - " + info + " - " + content + Config.NEW_LINE;
		System.out.println(out);
		FileWriter writer = null;
		try {
			// 打开一个写文件器，构造函数中的第二个参数true表示以追加形式写文件
			writer = new FileWriter(Config.SYSTEM_LOG, true);
			writer.write(out);
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (writer != null) {
					writer.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

}
