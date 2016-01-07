package jgh.elastic;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

public class JsonUtil {
    /**
     * 实现将实体对象转换成json对象
     * 
     * @param medicine
     *            Medicine对象
     * @return
     */
    public static String obj2JsonData(User user) {
	String jsonData = null;
	try {
	    // 使用XContentBuilder创建json数据
	    XContentBuilder jsonBuild = XContentFactory.jsonBuilder();
	    jsonBuild.startObject().field("grade", user.getGrade()).field("class", user.getGclass()).field("name", user.getName()).endObject();
	    jsonData = jsonBuild.string();
	    System.out.println(jsonData);
	} catch (IOException e) {
	    e.printStackTrace();
	}
	return jsonData;
    }
    
    public static List<String> getInitJsonData(){
        List<String> list = new ArrayList<String>();
        String data1  = JsonUtil.obj2JsonData(new User(7,5,"test1"));
        String data2  = JsonUtil.obj2JsonData(new User(8,5,"test2"));
        String data3  = JsonUtil.obj2JsonData(new User(9,6,"test3"));
        String data4  = JsonUtil.obj2JsonData(new User(10,7,"test4"));
        list.add(data1);
        list.add(data2);
        list.add(data3);
        list.add(data4);
        return list;
    }
}
