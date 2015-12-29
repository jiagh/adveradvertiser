package jgh.sql;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

import jgh.util.MysqlUtil;

// TODO: Auto-generated Javadoc
/**
 * The Class SqlUtil.
 */
public class SqlUtil {

    /**
     * 读取mysql表中展现项，统计项，条件项等进行sql查询语句拼接
     *
     * @param sqlMap
     *            the sql map
     * @return the hash map
     */
    public static HashMap<String, String> builderSql(HashMap<String, Object> sqlMap) {
	StringBuilder builder = new StringBuilder();
	HashMap<String, String> returnMap = new HashMap<String, String>();
	builder.append("SELECT ");
	boolean dimension = null != sqlMap.get("dimension") && !"".equals(sqlMap.get("dimension"));
	boolean indexbool = null != sqlMap.get("index") && !"".equals(sqlMap.get("index"));
	builder.append(sqlMap.get("display"));
	if (dimension && indexbool) {
	    builder.append(",");
	    String index[] = String.valueOf(sqlMap.get("index")).split(",");
	    for (int i = 0; i < index.length; i++) {
		if (index[i].equals("fetch_")) {
		    index[i] = "SUM(CASE WHEN reqType=0 THEN 1 ELSE 0 END) AS fetch_ ";
		} else if (index[i].equals("filling")) {
		    index[i] = "SUM(CASE WHEN advertisersId>0 THEN 1 ELSE 0 END) AS filling ";
		} else if (index[i].equals("req")) {
		    index[i] = "SUM(CASE WHEN reqType=1 THEN 1 ELSE 0 END) AS req ";
		} else if (index[i].equals("imp")) {
		    index[i] = "SUM(CASE WHEN reqType=2 THEN 1 ELSE 0 END) AS imp ";
		} else if (index[i].equals("click")) {
		    index[i] = "SUM(CASE WHEN reqType=3 THEN 1 ELSE 0 END) AS click ";
		} else if (index[i].equals("unique_ip")) {
		    index[i] = "COUNT(DISTINCT ip) AS unique_ip ";
		} else if (index[i].equals("unique_uid")) {
		    index[i] = "COUNT(DISTINCT uid) AS unique_uid ";
		} else if (index[i].equals("price")) {
		    index[i] = "SUM(price) AS price ";
		}
		builder.append(Arrays.toString(index).replaceAll("\\[|\\]", ""));
	    }
	}
	builder.append(" FROM ").append(sqlMap.get("table_name"));
	String conditStr = String.valueOf(sqlMap.get("condition"));
	if (conditStr.trim().length() > 0) {
	    builder.append(" WHERE ");
	    String cond[] = conditStr.split(",");
	    for (String c : cond) {
		builder.append(c).append(" AND ");
	    }
	    builder.delete(builder.lastIndexOf("AND"), builder.length());
	}
	String resultField = String.valueOf(sqlMap.get("display"));
	if (dimension) {
	    builder.append(" GROUP BY ").append(sqlMap.get("dimension"));
	    if (indexbool) {
		resultField += "," + String.valueOf(sqlMap.get("index"));
	    }
	}
	returnMap.put("resultField", resultField);
	returnMap.put("extuceSql", builder.toString());
	return returnMap;
    }

    /**
     * 拼接建表语句
     *
     * @param displayMap
     *            the display map
     * @return the string
     */
    public static String createTable(HashMap<String, Object> displayMap) {
	MysqlUtil mu = new MysqlUtil();
	StringBuilder displayBuilder = new StringBuilder();

	StringBuilder createTableBuilder = new StringBuilder();
	String dropExistsTableSql = "DROP TABLE IF EXISTS offline_basic_analysis.report_" + displayMap.get("id");
	mu.insertSql(dropExistsTableSql);
	String display[];
	createTableBuilder.append("CREATE TABLE offline_basic_analysis.report_").append(displayMap.get("id")).append("(id int(11) NOT NULL AUTO_INCREMENT ,");
	if (null != displayMap.get("index") && !"".equals(displayMap.get("index"))) {
	    displayBuilder.append(displayMap.get("index"));
	    display = displayBuilder.toString().split(",");
	    for (String dis : display) {
		if (dis.equals("price")) {
		    createTableBuilder.append(dis).append(" BIGINT(20) DEFAULT 0,");
		} else {
		    createTableBuilder.append(dis).append(" INT(11) DEFAULT 0,");
		}

	    }
	    displayBuilder.delete(0, displayBuilder.length());
	}
	displayBuilder.append(displayMap.get("display"));
	display = displayBuilder.toString().split(",");
	for (String dis : display) {
	    createTableBuilder.append(dis).append(" VARCHAR(255) DEFAULT '',");
	}
	createTableBuilder.append("PRIMARY KEY (id)) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;");
	mu.insertSql(createTableBuilder.toString());
	return createTableBuilder.toString();
    }

}
