package jgh.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;

import config.Config;

public class MysqlUtil {

    // 链接
    private Connection conn = null;
    // 返回集合
    private ResultSet rs = null;
    //
    private Statement sm = null;
    // URL
    private String url = Config.MYSQL_ADDRESS + "/";
    // MYSQL用户名
    private String username = Config.MYSQL_USERNAME;
    // MYSQL密码
    private String password = Config.MYSQL_PASSWORD;
    // MYSQL库名
    private String databasename = Config.MYSQL_DBNAME_BASIC_ANALYSIS;

    /**
     * 连接数据库 自定义参数
     */
    public void mysqlConn(String ip, String port, String username, String password, String dbname)
	    throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException {
	close();
	Class.forName("com.mysql.jdbc.Driver").newInstance();
	conn = DriverManager
		.getConnection("jdbc:mysql://" + ip + ":" + port + "/" + dbname + "?user=" + username + "&password=" + password + "&characterEncoding=utf8&useCompression=true");

    }

    /**
     * 连接数据库
     */
    public void mysqlConn() throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException {

	if (conn == null) {
	    Class.forName("com.mysql.jdbc.Driver").newInstance();
	    conn = DriverManager.getConnection("jdbc:mysql://" + url + databasename + "?user=" + username + "&password=" + password
		    + "&characterEncoding=utf8&useCompression=true&zeroDateTimeBehavior=convertToNull");
	}
    }

    /**
     * ResultSet To ArrayList
     */
    public ArrayList<HashMap<String, Object>> ResultSetMetaDataToArrayList(ResultSet rs) throws SQLException {
	ArrayList<HashMap<String, Object>> list = new ArrayList<HashMap<String, Object>>();
	ResultSetMetaData rsmd = rs.getMetaData();
	// 取得总条目数
	int columnCount = rsmd.getColumnCount();
	while (rs.next()) {
	    HashMap<String, Object> ht = new HashMap<String, Object>();
	    for (int i = 1; i <= columnCount; i++) {
		ht.put(rsmd.getColumnName(i).toLowerCase(), rs.getString(rsmd.getColumnName(i)));
	    }
	    list.add(ht);
	}
	return list;
    }

    /**
     * SELECT 操作
     */
    public ArrayList<HashMap<String, Object>> selectSql(String sql) {
	try {
	    System.out.println(sql);
	    // 链接数据库
	    mysqlConn();
	    if (sm == null)
		sm = conn.createStatement();
	    rs = sm.executeQuery(sql);

	    return ResultSetMetaDataToArrayList(rs);

	} catch (Exception e) {
	    e.printStackTrace();
	    return null;
	}finally{
	    close();
	}
    }

    /**
     * 添加 修改 删除 操作
     */
    public int insertSql(String sql) {

	try {
	    // 链接数据库
	    mysqlConn();
	    if (sm == null)
		sm = conn.createStatement();
	    return sm.executeUpdate(sql);
	} catch (Exception e) {
	    e.printStackTrace();
	    return -1;
	}finally{
	    close();
	}
    }

    /**
     * 批量插入
     * @param sqlList
     * @return
     */
    public int insertListSql(ArrayList<String> sqlList) {
	try {
	    mysqlConn();
	    conn.setAutoCommit(false);
	    sm = conn.createStatement();
	    int flag=1;
	    for (String sql : sqlList) {
		flag++;
		sm.addBatch(sql);
		if(flag==300){
		    sm.executeBatch();
		    conn.commit();
		    flag=0;
		}
	    }
	    sm.executeBatch();
	    conn.commit();
	    conn.setAutoCommit(true);   
	    return 1;
	}  catch (Exception e) {
	    e.printStackTrace();
	    return -1;
	}finally{
	    close();
	}

    }

    /**
     * 关闭链接
     */
    public void close() {
	try {
	    if (rs != null) {
		rs.close();
		rs = null;
	    }
	    if (sm != null) {
		sm.close();
		sm = null;
	    }
	    if (conn != null) {
		conn.close();
		conn = null;
	    }
	} catch (Exception ex) {
	    ex.printStackTrace();
	}
    }
}
