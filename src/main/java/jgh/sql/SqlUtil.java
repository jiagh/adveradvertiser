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
     * Builder sql.
     *
     * @param sqlMap
     *            the sql map
     * @return the hash map
     */
    public static HashMap<String, String> builderSql(HashMap<String, Object> sqlMap) {
	StringBuilder builder = new StringBuilder();
	HashMap<String, String> returnMap = new HashMap<String, String>();
	builder.append("select ");
	boolean dimension = null != sqlMap.get("dimension") && !"".equals(sqlMap.get("dimension"));
	boolean indexbool = null != sqlMap.get("index") && !"".equals(sqlMap.get("index"));
	builder.append(sqlMap.get("display"));
	if (dimension && indexbool) {
	    builder.append(",");
	    String index[] = String.valueOf(sqlMap.get("index")).split(",");
	    for (int i = 0; i < index.length; i++) {
		if (index[i].equals("fetch_")) {
		    index[i] = "sum(case when reqType=0 then 1 else 0 end) as fetch_ ";
		} else if (index[i].equals("filling")) {
		    index[i] = "sum(case when advertisersId>0 then 1 else 0 end) as filling ";
		} else if (index[i].equals("req")) {
		    index[i] = "sum(case when reqType=1 then 1 else 0 end) as req ";
		} else if (index[i].equals("imp")) {
		    index[i] = "sum(case when reqType=2 then 1 else 0 end) as imp ";
		} else if (index[i].equals("click")) {
		    index[i] = "sum(case when reqType=3 then 1 else 0 end) as click ";
		} else if (index[i].equals("unique_ip")) {
		    index[i] = "count(distinct ip) as unique_ip ";
		} else if (index[i].equals("unique_uid")) {
		    index[i] = "count(distinct uid) as unique_uid ";
		}else if (index[i].equals("price")) {
		    index[i] = "sum(price) as price ";
		}
		builder.append(Arrays.toString(index).replaceAll("\\[|\\]", ""));
	    }
	}
	
	builder.append(" from ").append(sqlMap.get("table_name"));
	String conditStr = String.valueOf(sqlMap.get("condition"));

	if (conditStr.trim().length() > 0) {
	    builder.append(" where ");
	    String cond[] = conditStr.split(",");
	    for (String c : cond) {
		builder.append(c).append(" and ");
	    }
	    builder.delete(builder.lastIndexOf("and"), builder.length());
	}
	String resultField= String.valueOf(sqlMap.get("display"));
	if(dimension){
	    builder.append(" group by ").append(sqlMap.get("dimension"));
	    if(indexbool){
		resultField+= ","+String.valueOf(sqlMap.get("index"));
	    }
	}
	returnMap.put("resultField",  resultField);
	returnMap.put("extuceSql", builder.toString());
	return returnMap;
    }

    /**
     * Creates the table.
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
		createTableBuilder.append(dis).append(" int(11) DEFAULT 0,");
	    }
	    displayBuilder.delete(0, displayBuilder.length());
	}

	displayBuilder.append(displayMap.get("display"));
	display = displayBuilder.toString().split(",");
	for (String dis : display) {
	    createTableBuilder.append(dis).append(" varchar(255) DEFAULT '',");
	}
	createTableBuilder.append("PRIMARY KEY (id)) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;");
	mu.insertSql(createTableBuilder.toString());
	return createTableBuilder.toString();
    }

    /**
     * The main method.
     *
     * @param args
     *            the arguments
     */
    public static void main(String args[]) {
	MysqlUtil mu = new MysqlUtil();

	ArrayList<String> sqlList=new ArrayList<String>();
	sqlList.add("insert into report_4(date,country,agent,ref,city) values('2015-12-10','局域网','python-requests/2.8.1','null','');");
	sqlList.add("insert into report_4(date,country,agent,ref,city) values('2015-12-10','局域网','python-requests/2.8.1','null','');");
	sqlList.add("insert into report_4(date,country,agent,ref,city) values('2015-12-10','局域网','python-requests/2.8.1','null','');");
	sqlList.add("insert into report_4(date,country,agent,ref,city) values('2015-12-10','局域网','python-requests/2.8.1','null','');");
	sqlList.add("insert into report_4(date,country,agent,ref,city) values('2015-12-10','局域网','python-requests/2.8.1','null','');");
	sqlList.add("insert into report_4(date,country,agent,ref,city) values('2015-12-10','局域网','python-requests/2.8.1','null','');");
	mu.insertListSql(sqlList);
	// ArrayList<HashMap<String, Object>> list = mu.selectSql("select * from
	// offline_basic_analysis.customize_report_info ");
	// for (HashMap<String, Object> map : list) {
	//// HashMap<String, String> sqlMap = builderSql(map);
	//// System.out.println(createTable(map));
	// }
    }

}
