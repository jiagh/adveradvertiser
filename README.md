### 版本
	v2.0.0
	
### SPARK3种启动模式
    --master yarn-cluster \
    --master yarn-client \
    --master local[2] \

### 日志类型目前有1种
    ReqLog
    reqtype = 1 请求
    reqtype = 2 展示
    reqtype = 3 点击
    
### v1.1.5 功能流程如下

##	写日志
	KafkaToHdfs
##	记分牌
	TopScoreBoard
##	日志过滤规则
    FilterLog
##	广告主地域报表分析
	advertisers_area_report
##	广告主项目报表分析
	advertisers_project_report
##	网站主收入报表分析
	publisher_income_report
##	网站主创意报表分析
	publisher_creative_report
##	扣费报表分析
	admin_all_advertisers_cost_report
##	管理员日报表
	admin_all_day_report
##	管理员广告项目报表
	admin_all_project_report
##	管理员地域报表
	admin_all_area_report
##	每日合并日志到HDFS
	MergeHdfsLogRun
##	每日合并数据报表数据
	MergeTable
##	分析后结果入库时会自动创建表或修改字段
	OrganizationTableOperating
##	报表修复功能
	FixOfflineAnalysisRun
	
### v2.0.0 功能流程如下

## 频次统计

## 算法数据支持

## DMP报表整合到管理员界面

###	KafkaToHdfs启动命令
    /usr/local/spark/bin/spark-submit \
    --class realtime.KafkaDataToHdfs \
    --master yarn-client \
    --num-executors 4 \
    --driver-memory 6g \
    --executor-memory 4g \
    --executor-cores 1 \
    /usr/local/spark/adnet-da-report-v2.0.0.jar 60 KafkaDataToHdfs Req 2 2 \

### ReportAnalysis启动命令
    /usr/local/spark/bin/spark-submit \
    --class offline.OfflineAnalysisRun \
    --master yarn-client \
    --num-executors 6 \
    --driver-memory 4g \
    --executor-memory 2g \
    --executor-cores 1 \
    /usr/local/spark/adnet-da-report-v2.0.0.jar \

### FixReportAnalysisi启动命令
	/usr/local/spark/bin/spark-submit \
    --class offline.FixOfflineAnalysisRun \
    --master yarn-client \
    --num-executors 6 \
    --driver-memory 4g \
    --executor-memory 2g \
    --executor-cores 1 \
   /usr/local/spark/adnet-da-report-v2.0.0.jar day 20151116 \

### SPARK SQL 自定义查询
    /usr/local/spark/bin/spark-submit \
    --class ManuallyAnalysis \
    --master yarn-client \
    --num-executors 6 \
    --driver-memory 4g \
    --executor-memory 2g \
    --executor-cores 1 \
    /usr/local/spark/adnet-da-report-v2.0.0.jar /adnet-da-report/logsBak/reqLog/2015/12/01/ "SELECT pageUriSplitParameter,SUM(fetch) as fetch FROM df WHERE slotId='ct-11578131-1' GROUP BY pageUriSplitParameter ORDER BY fetch DESC" \

### 合并表
	/usr/local/spark/bin/spark-submit \
    --class handle.MergeTable \
    --master yarn-client \
    --num-executors 6 \
    --driver-memory 4g \
    --executor-memory 2g \
    --executor-cores 1 \
    /usr/local/spark/adnet-da-report-v2.0.0.jar 2015-12-04 \

### HIVE JDBC 启动
	hive --service hiveserver

### HIVE
	CREATE DATABASE adnet_da_report;
	CREATE TABLE adnet_da_report.cogtu_log (log STRING) PARTITIONED BY (date date);

### HIVE UDF
	ADD JAR /usr/local/spark/adnet-da-report-v1.1.5.jar;
	CREATE TEMPORARY FUNCTION reqs_parse_array_udf AS 'hive.udf.ReqsParseArrayListUdf';

### 查询
	SELECT * FROM adnet_da_report.cogtu_log LIMIT 10;
	SELECT * FROM adnet_da_report.cogtu_log_view LIMIT 10;

###	HIVE 解析日志视图
	DROP VIEW adnet_da_report.cogtu_log_view;
	CREATE VIEW adnet_da_report.cogtu_log_view AS SELECT cl.*,t3.*,from_unixtime(cast(substr(cl.timestamp,0,10) AS INT),'HH') as hour  FROM (SELECT t1.*,date FROM adnet_da_report.cogtu_log  LATERAL VIEW JSON_TUPLE(log,'ip','timestamp','slotId','siteId','publisherId','pageUri','ref','country','province','city','addrCode','browser','os','uid','runTime','vBalanceCostPrecent','v','sessionId','source','reqs','reqType') t1 AS ip,timestamp,slotId,siteId,publisherId,pageUri,ref,country,province,city,addrCode,browser,os,uid,runTime,vBalanceCostPrecent,v,sessionId,source,reqs,reqType) AS cl LATERAL VIEW EXPLODE(reqs_parse_array_udf(reqs)) t2 as arrjson LATERAL VIEW json_tuple(arrjson,"impId","advertisersId","projectId","campaignId","creativeId","templateId","price","priceType") t3 as impId,advertisersId,projectId,campaignId,creativeId,templateId,price,priceType