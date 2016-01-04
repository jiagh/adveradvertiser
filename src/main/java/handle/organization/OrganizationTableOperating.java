package handle.organization;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;

import util.MysqlUtil;

public class OrganizationTableOperating {

	/**
	 * 取得表结构
	 */
	public static ArrayList<String> tableStructure(String tableName, String className, String dbName, MysqlUtil db) throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException {

		// 返回集合
		ArrayList<String> re = new ArrayList<String>();
		// 读取表结构
		ArrayList<HashMap<String, Object>> list = db.selectSql("SELECT column_name FROM information_schema.columns WHERE table_name = '" + tableName + "' AND table_schema = '" + dbName + "'");
		// 循环读取字段
		for (int i = 0; i < list.size(); i++)
			// 装载表字段到返回集合
			re.add(list.get(i).get("column_name").toString());
		// 返回
		return re;

	}

}
