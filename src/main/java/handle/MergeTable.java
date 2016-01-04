package handle;

import java.lang.reflect.Field;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import config.Config;
import util.MysqlUtil;

public class MergeTable {

	public void go(String day) throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException {

		MysqlUtil db = new MysqlUtil();

		if (db.selectSql("SELECT 1 FROM offline_basic_analysis.merge_table_list WHERE day='" + day + "'  ").size() == 0) {

			// 广告主报表
			ArrayList<HashMap<String, Object>> adv = db.selectSql("SELECT * FROM " + Config.MYSQL_DBNAME_LOGIC + ".advertisers ");

			for (int i = 0; i < adv.size(); i++) {

				db.insertSql("INSERT INTO merge_table_list (day,status,user_id,user_type,table_name) VALUES ('" + day + "','N','" + adv.get(i).get("advertisers_id") + "','advertisers','Advertisers_Area_Report')");
				db.insertSql("INSERT INTO merge_table_list (day,status,user_id,user_type,table_name) VALUES ('" + day + "','N','" + adv.get(i).get("advertisers_id") + "','advertisers','Advertisers_Project_Report')");

			}

			// 网站主报表
			ArrayList<HashMap<String, Object>> pub = db.selectSql("SELECT * FROM " + Config.MYSQL_DBNAME_LOGIC + ".publisher ");

			for (int i = 0; i < pub.size(); i++) {

				db.insertSql("INSERT INTO merge_table_list (day,status,user_id,user_type,table_name) VALUES ('" + day + "','N','" + pub.get(i).get("publisher_id") + "','publisher','Publisher_Creative_Report')");
				db.insertSql("INSERT INTO merge_table_list (day,status,user_id,user_type,table_name) VALUES ('" + day + "','N','" + pub.get(i).get("publisher_id") + "','publisher','Publisher_Income_Report')");
				db.insertSql("INSERT INTO merge_table_list (day,status,user_id,user_type,table_name) VALUES ('" + day + "','N','" + pub.get(i).get("publisher_id") + "','publisher','Publisher_Slot_Report')");
			}

			// 管理员
			
			db.insertSql("INSERT INTO merge_table_list (day,status,user_id,user_type,table_name) VALUES ('" + day + "','N','-1','admin','All_Advertisers_Cost_Report')");
			db.insertSql("INSERT INTO merge_table_list (day,status,user_id,user_type,table_name) VALUES ('" + day + "','N','-1','admin','All_Day_Report')");
			db.insertSql("INSERT INTO merge_table_list (day,status,user_id,user_type,table_name) VALUES ('" + day + "','N','-1','admin','All_Project_Report')");
			db.insertSql("INSERT INTO merge_table_list (day,status,user_id,user_type,table_name) VALUES ('" + day + "','N','-1','admin','All_Area_Report')");

		}

		try {

			ArrayList<HashMap<String, Object>> info = db.selectSql("SELECT * FROM offline_basic_analysis.merge_table_list WHERE status = 'N'");

			for (int i = 0; i < info.size(); i++) {

				String className = "";
				String tableName = "";
				String uidTableName = "";
				String tableNameMerge = "";

				if (info.get(i).get("user_type").equals("admin")) {
					tableName = "admin_" + info.get(i).get("table_name").toString().toLowerCase();
					className = "Admin_" + info.get(i).get("table_name").toString();
					uidTableName = "admin_" + info.get(i).get("table_name").toString().toLowerCase();
					tableNameMerge = "admin_" + info.get(i).get("table_name").toString().toLowerCase() + "_" + info.get(i).get("day").toString().replaceAll("-", "") + "_merge";
				} else {
					tableName = info.get(i).get("table_name").toString().toLowerCase();
					className = info.get(i).get("table_name").toString();
					uidTableName = info.get(i).get("user_id") + "_" + info.get(i).get("table_name").toString().toLowerCase();
					tableNameMerge = info.get(i).get("user_id") + "_" + info.get(i).get("table_name").toString().toLowerCase() + "_" + info.get(i).get("day").toString().replaceAll("-", "") + "_merge";
				}

				String dbName = db.selectSql("SELECT dbname FROM user_table_location WHERE table_name =  '" + tableName + "'").get(0).get("dbname").toString();

				// 连接广告主所在MYSQL服务器参数
				ArrayList<HashMap<String, Object>> userdb = db
						.selectSql("SELECT ip,port,username,password FROM user_db_location WHERE userid='" + info.get(i).get("user_id") + "' AND user_type = '" + info.get(i).get("user_type") + "' AND dbname = '" + dbName + "'");
				// 重新连接新的库
				db.mysqlConn(userdb.get(0).get("ip").toString(), userdb.get(0).get("port").toString(), userdb.get(0).get("username").toString(), userdb.get(0).get("password").toString(), dbName);

				// 合并原始表数据到临时表
				String sqlStep1 = "CREATE TABLE " + tableNameMerge + " SELECT ";

				// 反射
				Class<?> key = Class.forName("vo.report.key." + info.get(i).get("user_type") + "." + className + "_Key").newInstance().getClass();
				Field[] fieldsKey = key.getDeclaredFields();
				for (int j = 0; j < fieldsKey.length; j++) {
					sqlStep1 += fieldsKey[j].getName() + ",";
				}
				// 反射
				Class<?> value = Class.forName("vo.report.value." + info.get(i).get("user_type") + "." + className + "_Value").newInstance().getClass();
				Field[] fieldsValue = value.getDeclaredFields();
				for (int j = 0; j < fieldsValue.length; j++) {
					sqlStep1 += "SUM(" + fieldsValue[j].getName() + ") AS " + fieldsValue[j].getName() + ",";
				}

				sqlStep1 += "'merge' AS complete_folder_name";
				sqlStep1 += " FROM " + uidTableName + "  WHERE day = '" + info.get(i).get("day") + "' GROUP BY ";
				for (int j = 0; j < fieldsKey.length; j++) {
					sqlStep1 += fieldsKey[j].getName() + ",";
				}
				sqlStep1 = (sqlStep1.substring(0, sqlStep1.length() - 1));

				// 删除原始表数据
				String sqlStep2 = "DELETE FROM " + uidTableName + " WHERE day = '" + info.get(i).get("day") + "'";

				// 插入临时表到原始表
				String sqlStep3 = "INSERT INTO " + uidTableName + " (";
				for (int j = 0; j < fieldsKey.length; j++) {
					sqlStep3 += fieldsKey[j].getName() + ",";
				}
				for (int j = 0; j < fieldsValue.length; j++) {
					sqlStep3 += fieldsValue[j].getName() + ",";
				}
				sqlStep3 += "complete_folder_name) " + "SELECT ";
				for (int j = 0; j < fieldsKey.length; j++) {
					sqlStep3 += fieldsKey[j].getName() + ",";
				}
				for (int j = 0; j < fieldsValue.length; j++) {
					sqlStep3 += fieldsValue[j].getName() + ",";
				}
				sqlStep3 += "complete_folder_name FROM " + tableNameMerge;

				String sqlStep4 = "DROP TABLE " + tableNameMerge;
				String sqlStep5 = "UPDATE merge_table_list SET status = 'Y' WHERE id = " + info.get(i).get("id");

				db.insertSql(sqlStep1);
				db.insertSql(sqlStep2);
				db.insertSql(sqlStep3);
				db.insertSql(sqlStep4);
				db.insertSql(sqlStep5);

			}

		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	public static void main(String[] args) throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException {
		new MergeTable().go(args[0]);
	}
}
