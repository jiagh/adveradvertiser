package handle;

import java.sql.SQLException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map.Entry;

import config.Config;
import util.DateUtil;
import util.MysqlUtil;

public class CheckAndCreateTableInfo {

	public static void main(String[] args) throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException {
		new CheckAndCreateTableInfo();
	}

	public CheckAndCreateTableInfo() throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException {

		MysqlUtil db = new MysqlUtil();

		ArrayList<HashMap<String, Object>> advid = db.selectSql("SELECT advertisers_id FROM " + Config.MYSQL_DBNAME_LOGIC + ".advertisers");
		ArrayList<HashMap<String, Object>> pubid = db.selectSql("SELECT publisher_id FROM " + Config.MYSQL_DBNAME_LOGIC + ".publisher");
		ArrayList<HashMap<String, Object>> advTableNames = db.selectSql("SELECT table_name,class_name FROM " + Config.MYSQL_DBNAME_BASIC_ANALYSIS + ".user_table_location WHERE user_type ='advertisers'");
		ArrayList<HashMap<String, Object>> pubTableNames = db.selectSql("SELECT table_name,class_name FROM " + Config.MYSQL_DBNAME_BASIC_ANALYSIS + ".user_table_location WHERE user_type ='publisher'");
		ArrayList<HashMap<String, Object>> admTableNames = db.selectSql("SELECT table_name,class_name FROM " + Config.MYSQL_DBNAME_BASIC_ANALYSIS + ".user_table_location WHERE user_type ='admin'");

		for (int i = 0; i < advid.size(); i++) {
			for (int j = 0; j < advTableNames.size(); j++) {
				// 检查表
				checkTableInfo(advid.get(i).get("advertisers_id") + "_" + advTableNames.get(j).get("table_name").toString(), advTableNames.get(j).get("class_name").toString(), advid.get(i).get("advertisers_id").toString(), "advertisers");
			}
		}

		for (int i = 0; i < pubid.size(); i++) {
			for (int j = 0; j < pubTableNames.size(); j++) {
				// 检查表
				checkTableInfo(pubid.get(i).get("publisher_id") + "_" + pubTableNames.get(j).get("table_name").toString(), pubTableNames.get(j).get("class_name").toString(), pubid.get(i).get("publisher_id").toString(), "publisher");
			}
		}

		for (int j = 0; j < admTableNames.size(); j++) {
			// 检查表
			checkTableInfo(admTableNames.get(j).get("table_name").toString(), admTableNames.get(j).get("class_name").toString(), "-1", "admin");

		}

	}

	/**
	 * 创建报表的时候按照月来分区分表
	 */
	public static String tablePartitionMonth() {

		String expand_artition = "PARTITION BY RANGE (TO_DAYS(`day`))" + "(";

		for (int i = 1; i < 24; i++) {

			String date = "";

			try {

				DateFormat df = new SimpleDateFormat("yyyy-MM-dd");

				Calendar c1 = Calendar.getInstance();
				// 得到当前日期
				c1.setTime(df.parse(DateUtil.getNowDate(DateUtil.yyyy_MM_dd_HH_mm_ss)));
				// 得到后一个月的日期
				c1.add(Calendar.MONTH, +i);
				//
				date = df.format(c1.getTime());

			} catch (ParseException e) {
				e.printStackTrace();
			}

			if (i == 23) {
				expand_artition += "PARTITION p" + date.substring(0, 7).replaceAll("-", "") + " VALUES LESS THAN MAXVALUE );";
			} else {
				expand_artition += "PARTITION p" + date.substring(0, 7).replaceAll("-", "") + " VALUES LESS THAN (TO_DAYS('" + date.substring(0, 7) + "-01" + "')),";
			}
		}

		return expand_artition;
	}

	/**
	 * 创建表和修改表
	 */
	public static void checkTableInfo(String tableName, String className, String user_id, String user_type) throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException {

		MysqlUtil db = new MysqlUtil();

		LinkedHashMap<String, String> fieldList = new LinkedHashMap<String, String>();

		String tableNameReal = "";
		String packageNameKey = "";
		String packageNameValue = "";

		if (tableName.split("_")[0].indexOf("admin") != -1) {
			packageNameKey = "vo.report.key.admin.";
			packageNameValue = "vo.report.value.admin.";
			tableNameReal = tableName;
		} else if (tableName.split("_")[1].indexOf("advertisers") != -1) {
			packageNameKey = "vo.report.key.advertisers.";
			packageNameValue = "vo.report.value.advertisers.";
			tableNameReal = tableName.split("_", 2)[1];
		} else if (tableName.split("_")[1].indexOf("publisher") != -1) {
			packageNameKey = "vo.report.key.publisher.";
			packageNameValue = "vo.report.value.publisher.";
			tableNameReal = tableName.split("_", 2)[1];
		}

		String dbName = db.selectSql("SELECT dbname FROM " + Config.MYSQL_DBNAME_BASIC_ANALYSIS + ".user_table_location WHERE table_name =  '" + tableNameReal + "'").get(0).get("dbname").toString();
		// 连接广告主所在MYSQL服务器参数
		ArrayList<HashMap<String, Object>> userdb = db.selectSql("SELECT ip,port,username,password FROM user_db_location WHERE userid=" + user_id + " AND user_type = '" + user_type + "' AND dbname = '" + dbName + "'");
		// 重新连接新的库
		db.mysqlConn(userdb.get(0).get("ip").toString(), userdb.get(0).get("port").toString(), userdb.get(0).get("username").toString(), userdb.get(0).get("password").toString(), dbName);

		String index = "";

		// Key的列
		Class<?> key = Class.forName(packageNameKey + className + "_Key");
		for (int i = 0; i < key.getDeclaredFields().length; i++) {
			fieldList.put(key.getDeclaredFields()[i].getName(), key.getDeclaredFields()[i].getGenericType().toString());
			index += "KEY `" + key.getDeclaredFields()[i].getName() + "` (`" + key.getDeclaredFields()[i].getName() + "`),";
		}

		// Value的列
		Class<?> value = Class.forName(packageNameValue + className + "_Value");
		for (int i = 0; i < value.getDeclaredFields().length; i++)
			fieldList.put(value.getDeclaredFields()[i].getName(), value.getDeclaredFields()[i].getGenericType().toString());

		//
		ArrayList<HashMap<String, Object>> tableIsExist = db.selectSql("SELECT table_name FROM information_schema.TABLES WHERE TABLE_NAME='" + tableName + "' AND TABLE_SCHEMA= '" + dbName + "'");

		// 如果表不存在
		if (tableIsExist.size() == 0) {

			// 组织建表语句
			String createSql = "CREATE TABLE `" + tableName + "` ( ";

			// 循环取得列和类型
			Iterator<Entry<String, String>> it = fieldList.entrySet().iterator();

			while (it.hasNext()) {

				Entry<String, String> en = it.next();

				createSql += "  `" + en.getKey() + "` ";

				// 日期类型的强制为DATE
				if (en.getKey().indexOf("day") != -1 || en.getKey().indexOf("date") != -1) {
					createSql += " date DEFAULT NULL, ";
				} else {

					if (en.getValue().indexOf("String") != -1)
						createSql += " varchar(255) DEFAULT NULL, ";
					else if (en.getValue().indexOf("int") != -1)
						createSql += " int(11) DEFAULT NULL, ";
					else if (en.getValue().indexOf("long") != -1)
						createSql += " bigint(20) DEFAULT NULL, ";
					else if (en.getValue().indexOf("double") != -1)
						createSql += " double DEFAULT NULL, ";

				}
			}

			createSql += " `complete_folder_name` varchar(255) DEFAULT NULL, " + index.substring(0, index.length() - 1) + " ) ENGINE=MyISAM DEFAULT CHARSET=utf8 ";

			System.out.println(createSql);

			// 创建
			db.insertSql(createSql + " " + tablePartitionMonth());

		} else { // 表存在的话需要判断字段的增删改

			// 取得表的字段结构
			ArrayList<HashMap<String, Object>> tableColumn = db.selectSql("SELECT column_name,data_type FROM information_schema.columns WHERE  TABLE_NAME='" + tableName + "' AND TABLE_SCHEMA= '" + dbName + "'");

			HashSet<String> mysqlTableColumn = new HashSet<String>();
			for (int i = 0; i < tableColumn.size(); i++)
				mysqlTableColumn.add(tableColumn.get(i).get("column_name").toString());

			System.out.println(mysqlTableColumn);

			Iterator<Entry<String, String>> fieldListIt = fieldList.entrySet().iterator();

			// 添加字段
			while (fieldListIt.hasNext()) {

				Entry<String, String> fieldListItEn = fieldListIt.next();

				// 如果Vo里的字段不存在表中则添加
				if (!mysqlTableColumn.contains(fieldListItEn.getKey())) {

					// 添加字段语句
					String alter = "ALTER  TABLE `" + tableName + "` ADD " + fieldListItEn.getKey();

					// Vo里变量的类型
					String type = fieldListItEn.getValue();

					// 日期类型的强制为DATE
					if (type.indexOf("day") != -1 || type.indexOf("date") != -1) {
						alter += " date DEFAULT NULL, ";
					} else {
						if (type.indexOf("String") != -1)
							alter += " varchar(255) DEFAULT NULL ";
						else if (type.indexOf("int") != -1)
							alter += " int(11) DEFAULT NULL ";
						else if (type.indexOf("long") != -1)
							alter += " bigint(20) DEFAULT NULL ";
						else if (type.indexOf("double") != -1)
							alter += " double DEFAULT NULL ";
					}

					// 创建字段
					db.insertSql(alter);

				}
			}

			// 修改字段
			for (int i = 0; i < tableColumn.size(); i++) {

				// 数据库表列名
				String columnName = tableColumn.get(i).get("column_name").toString();

				String alter = "ALTER  TABLE `" + tableName + "` MODIFY COLUMN  " + columnName + " ";

				if (fieldList.containsKey(columnName)) {

					// BEAN的类型
					String type = fieldList.get(columnName);

					// 日期类型的强制为DATE
					if (columnName.indexOf("day") != -1 || columnName.indexOf("date") != -1) {
						type = "date";
					} else {
						if (type.indexOf("String") != -1)
							type = "varchar";
						else if (type.indexOf("int") != -1)
							type = "int";
						else if (type.indexOf("long") != -1)
							type = "bigint";
						else if (type.indexOf("double") != -1)
							type = "double";
					}

					if (!tableColumn.get(i).get("data_type").toString().equals(type)) {

						if (type.indexOf("varchar") != -1)
							alter += " varchar(255) DEFAULT NULL ";
						else if (type.indexOf("bigint") != -1)
							alter += " bigint(20) DEFAULT NULL ";
						else if (type.indexOf("int") != -1)
							alter += " int(11) DEFAULT NULL ";
						else if (type.indexOf("double") != -1)
							alter += " double DEFAULT NULL ";

						// 修改字段类型
						db.insertSql(alter);
					}
				}
			}

			// 删除字段
			for (String mysqlTableColumnString : mysqlTableColumn) {

				if (!mysqlTableColumnString.equals("complete_folder_name") && !fieldList.containsKey(mysqlTableColumnString)) {
					// 删除字段
					db.insertSql("ALTER TABLE `" + tableName + "` DROP COLUMN  `" + mysqlTableColumnString + "`  ");
				}
			}
		}
	}

}
