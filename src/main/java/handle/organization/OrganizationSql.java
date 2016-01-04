package handle.organization;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import config.Config;
import util.JsonUtil;
import util.MysqlUtil;
import util.PublicUtil;

public class OrganizationSql {

	/**
	 * 生成并插入SQL语句
	 */
	public static String organizationSqlReportReduceOut(String key, Iterable<String> values, String complete_folder_name) throws IOException, InterruptedException, InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException,
			NoSuchFieldException, SecurityException, NoSuchMethodException, IllegalArgumentException, InvocationTargetException {

		StringBuffer re = new StringBuffer();

		// 用户ID
		String userId = key.split("_")[0];
		// 表名
		String tableName = key.toLowerCase();
		// 类名
		String className = "";
		// 该所属用户类型
		String userType = "";

		if (tableName.split("_")[0].equals("admin")) {
			// 管理员 默认ID = -1
			userId = "-1";
			userType = "admin";
			className = key;
		} else if (tableName.split("_")[1].equals("advertisers")) {
			userType = "advertisers";
			className = key.split("_", 2)[1];
		} else if (tableName.split("_")[1].equals("publisher")) {
			userType = "publisher";
			className = key.split("_", 2)[1];
		}

		MysqlUtil db = new MysqlUtil();

		String dbName = db.selectSql("SELECT dbname FROM user_table_location WHERE table_name =  '" + className.toLowerCase() + "'").get(0).get("dbname").toString();

		// 连接广告主所在MYSQL服务器参数
		ArrayList<HashMap<String, Object>> userdb = db.selectSql("SELECT ip,port,username,password FROM user_db_location WHERE userid='" + userId + "' AND user_type = '" + userType + "' AND dbname = '" + dbName + "'");

		if (userdb.size() == 0) {
			return "ERR";
		}

		// 重新连接新的库
		db.mysqlConn(userdb.get(0).get("ip").toString(), userdb.get(0).get("port").toString(), userdb.get(0).get("username").toString(), userdb.get(0).get("password").toString(), dbName);
		// 取得表的所有列
		ArrayList<String> fieldList = OrganizationTableOperating.tableStructure(tableName, className, dbName, db);
		// 迭代报表所有数据
		Iterator<String> it = values.iterator();

		String field = "";
		// 顺序排列
		for (int i = 0; i < fieldList.size(); i++) {
			// ID自增长不需要管
			if (fieldList.get(i).equals("id")) {
				continue;
			}
			field += fieldList.get(i) + ",";
		}

		StringBuffer sql = new StringBuffer("INSERT INTO " + tableName);

		// 完善语句
		sql.append(" (" + field.substring(0, field.length() - 1) + ") VALUES ");

		StringBuffer tmpSql = sql;

		// 循环所有需要入库的数据
		while (it.hasNext()) {

			// 所有字段的值
			HashMap<String, Object> fieldKeyValue = new HashMap<String, Object>();
			// 设置特殊字段的值
			fieldKeyValue.put("complete_folder_name", complete_folder_name);

			String itS = it.next();

			// 取得Key Vo 和 Value Vo
			String keyValue[] = itS.split(Config.REPORT_VALUE_DELIMITED);
			// Key 反射
			keyValueVoToHashMap(keyValue[0].trim(), "vo.report.key." + userType + "." + className + "_Key", fieldKeyValue);
			// Value 反射
			keyValueVoToHashMap(keyValue[1].trim(), "vo.report.value." + userType + "." + className + "_Value", fieldKeyValue);
			// SQL的值
			String value = "";

			// 顺序排列
			for (int i = 0; i < fieldList.size(); i++) {

				// ID自增长不需要管
				if (fieldList.get(i).equals("id")) {
					continue;
				}

				// 完善语句
				value += "'" + fieldKeyValue.get(fieldList.get(i)) + "',";
			}

			// 完善语句
			tmpSql.append(" (" + value.substring(0, value.length() - 1) + "),");

			// 超过长度就插入
			if (tmpSql.length() >= (1024 * 1024 * 9)) {

				//
				String tmp = tmpSql.toString().substring(0, tmpSql.lastIndexOf(","));
				// 输出
				re.append(tmp + Config.NEW_LINE);
				// 执行语句
				db.insertSql(tmp);
				// 重置
				tmpSql = sql;

			}
		}

		if (tmpSql.indexOf(",") != -1) {

			//
			String tmp = tmpSql.toString().substring(0, tmpSql.lastIndexOf(","));
			// 输出
			re.append(tmp + Config.NEW_LINE);
			// 执行语句
			db.insertSql(tmp);

		}

		return re.toString();
	}

	/**
	 * 反射KEY和VALUE
	 */
	public static HashMap<String, Object> keyValueVoToHashMap(String json, String class_, HashMap<String, Object> re)
			throws NoSuchMethodException, SecurityException, ClassNotFoundException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {

		// 反射
		Object obKey = JsonUtil.toBean(json, Class.forName(class_));

		Field[] fields = obKey.getClass().getDeclaredFields();

		for (int i = 0; i < fields.length; i++) {

			Method method = obKey.getClass().getDeclaredMethod("get" + PublicUtil.toUpperCaseFirstOne(fields[i].getName()));
			// 调用方法取得返回值
			Object ret = method.invoke(obKey);
			// 字段和值
			re.put(fields[i].getName(), ret);

		}
		return re;
	}

}
