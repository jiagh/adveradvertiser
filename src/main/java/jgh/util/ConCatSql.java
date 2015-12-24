package jgh.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

public class ConCatSql {

    public static HashMap<String, String> builderSql(HashMap<String, Object> sqlMap) {
	StringBuilder builder = new StringBuilder();
	HashMap<String, String> returnMap = new HashMap<String, String>();
	builder.append("select ");
	boolean indexBool=null!=sqlMap.get("index");
	if(indexBool){
	    String index[]=String.valueOf(sqlMap.get("index")).split(",");
		for(int i=0;i<index.length;i++){
		    if(index[i].equals("fetch_")){
			index[i] = "sum(case when reqType=0 then 1 else 0 end) as fetch_ ";
		    }else if(index[i].equals("filling")){
			index[i] = "sum(case when advertisersId>0 then 1 else 0 end) as filling ";
		    }else if(index[i].equals("req")){
			index[i] = "sum(case when reqType=1 then 1 else 0 end) as req ";
		    }else if(index[i].equals("imp")){
			index[i] = "sum(case when reqType=2 then 1 else 0 end) as imp ";
		    }else if(index[i].equals("click")){
			index[i] = "sum(case when reqType=3 then 1 else 0 end) as click ";
		    }else if(index[i].equals("unique_ip")){
			index[i] = "sum(distinct ip) as unique_ip ";
		    }else if(index[i].equals("unique_uid")){
			index[i] = "sum(distinct uid) as unique_uid ";
		    }
		}
		builder.append(Arrays.toString(index).replaceAll("\\[|\\]", "")).append(","); 
	}
	builder.append(sqlMap.get("display"));
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
	if(indexBool){
	    builder.append(" group by ").append(sqlMap.get("dimension"));  
	}
	returnMap.put("extuceSql", builder.toString());
	if(null==sqlMap.get("index")){
	    returnMap.put("resultField",String.valueOf(sqlMap.get("display")));
	}else{
	    returnMap.put("resultField", String.valueOf(sqlMap.get("index"))+","+sqlMap.get("display"));  
	}
	return returnMap;
    }

    public static void main(String args[]) {
	MysqlUtil mu = new MysqlUtil();
	ArrayList<HashMap<String, Object>> list = mu.selectSql("select * from offline_basic_analysis.customize_report_info where status='N'");
	for(HashMap<String,Object> map:list){
	    builderSql(map); 
	}
	// ccs.getData();
    }

}
