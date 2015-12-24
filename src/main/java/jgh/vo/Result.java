package jgh.vo;

import java.util.HashMap;

public class Result {

    public String getTableName() {
        return tableName;
    }
    public void setTableName(String tableName) {
        this.tableName = tableName;
    }
    public String getSpace() {
        return space;
    }
    public void setSpace(String space) {
        this.space = space;
    }
    public HashMap<String, String> getResultMap() {
        return resultMap;
    }
    public void setResultMap(HashMap<String, String> resultMap) {
        this.resultMap = resultMap;
    }
    public String getGroupField() {
        return groupField;
    }
    public void setGroupField(String groupField) {
        this.groupField = groupField;
    }

    public String getOrderField() {
        return orderField;
    }
    public void setOrderField(String orderField) {
        this.orderField = orderField;
    }
    public String getQueryField() {
        return queryField;
    }
    public void setQueryField(String queryField) {
        this.queryField = queryField;
    }
    private String tableName;
    private String space;
    private HashMap<String,String> resultMap;
    private String groupField;
    private String orderField;
    private String queryField;
}
