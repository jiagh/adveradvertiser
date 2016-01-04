##	版本
	v2.2.0

##SPARK 3种启动模式
	--master yarn-cluster \
	--master yarn-client \
	--master local[2] \

##	日志
    ReqLog
    reqtype =-1 问题日志 
    reqtype = 1 请求
    reqtype = 2 展示
    reqtype = 3 点击


##v1.1.0 功能流程如下
	写日志
	KafkaToHdfs
	记分牌
	TopScoreBoard
	广告主地域报表分析
	advertisers_area_report
	广告主项目报表分析
	advertisers_project_report
	网站主收入报表分析
	publisher_income_report
	网站主创意报表分析
	publisher_creative_report
	扣费报表分析
	admin_all_advertisers_cost_report
	管理员日报表
	admin_all_day_report
	管理员广告项目报表
	admin_all_project_report
	每日合并数据报表数据
	MergeTable
	分析后结果入库时会自动创建表或修改字段
	OrganizationTableOperating

##v1.1.5 功能流程如下
	管理员地域报表
	admin_all_area_report
	报表修复功能
	FixOfflineAnalysisRun
	每日合并日志到HDFS
	MergeHdfsLogRun
	日志过滤规则
	FilterLog

##v2.0.0 功能流程如下
	频次统计
	Frequency

##v2.2.0
	自定义报表分析
	SparkCustomizeReport
	算法数据支持
	AigorithmSupportRealTime


##Frequency启动命令 (服务)
    /usr/local/spark/bin/spark-submit \
    --class realtime.Frequency \
    --master yarn-client \
    --num-executors 4 \
    --driver-memory 6g \
    --executor-memory 4g \
    --executor-cores 1 \
    /usr/local/spark/adnet-da-report-v2.1.0.jar 1 Frequency Req 2 \

##算法用数据支持启动命令 (服务)
    /usr/local/spark/bin/spark-submit \
    --class realtime.AigorithmSupportRealTime \
    --master yarn-client \
    --num-executors 4 \
    --driver-memory 6g \
    --executor-memory 4g \
    --executor-cores 1 \
    /usr/local/spark/adnet-da-report-v2.1.0.jar 120 AigorithmSupportRealTime Req 2 \

##KafkaToHdfs启动命令 (服务)
    /usr/local/spark/bin/spark-submit \
    --class realtime.KafkaDataToHdfs \
    --master yarn-client \
    --num-executors 4 \
    --driver-memory 6g \
    --executor-memory 4g \
    --executor-cores 1 \
    /usr/local/spark/adnet-da-report-v2.1.0.jar 60 KafkaDataToHdfs Req 2 2 \

##ReportAnalysis启动命令 (服务)
    /usr/local/spark/bin/spark-submit \
    --class offline.OfflineAnalysisRun \
    --master yarn-client \
    --num-executors 6 \
    --driver-memory 4g \
    --executor-memory 2g \
    --executor-cores 1 \
    /usr/local/spark/adnet-da-report-v2.1.0.jar \

##FixReportAnalysisi启动命令 (工具)
    /usr/local/spark/bin/spark-submit \
    --class offline.FixOfflineAnalysisRun \
    --master yarn-client \
    --num-executors 6 \
    --driver-memory 4g \
    --executor-memory 2g \
    --executor-cores 1 \
    /usr/local/spark/adnet-da-report-v2.1.0.jar day 20151116 \

##SPARK SQL 自定义查询 (工具)
    /usr/local/spark/bin/spark-submit \
    --class ManuallyAnalysis \
    --master yarn-client \
    --num-executors 6 \
    --driver-memory 4g \
    --executor-memory 2g \
    --executor-cores 1 \
    /usr/local/spark/adnet-da-report-v2.1.0.jar /adnet-da-report/logsBak/reqLog/2015/12/01/ "SELECT pageUriSplitParameter,SUM(fetch) as fetch FROM df WHERE slotId='ct-11578131-1' GROUP BY pageUriSplitParameter ORDER BY fetch DESC" \

##合并日志 (工具)
    /usr/local/spark/bin/spark-submit \
    --class mapreduce.MergeHdfsLogRun \
    --master yarn-client \
    --num-executors 6 \
    --driver-memory 4g \
    --executor-memory 2g \
    --executor-cores 1 \
    /usr/local/spark/adnet-da-report-v2.1.0.jar 2015-12-19 \

##合并表 (工具)
    /usr/local/spark/bin/spark-submit \
    --class handle.MergeTable \
    --master yarn-client \
    --num-executors 6 \
    --driver-memory 4g \
    --executor-memory 2g \
    --executor-cores 1 \
    /usr/local/spark/adnet-da-report-v2.1.0.jar 2015-12-18 \

##SPARK SQL CLI 启动 (工具)
    spark-sql --jars /usr/local/spark/adnet-da-report-v2.1.0.jar --master yarn --num-executors 10  --driver-memory 4G --executor-memory 3G


##SQL 获取所有点击过广告的的用户的 最近7天的所有访问URL (算法支持用) (工具)
    /usr/local/spark/bin/spark-submit \
    --class spark.sql.SparkSqlQuery \
    --master yarn-client \
    --num-executors 20  \
    --driver-memory 2g \
    --executor-memory 2g --executor-cores 1 /usr/local/spark/adnet-da-report-v2.1.0.jar \
    " \
    SELECT B.uid,sort_array_timestamp_return_url_array_udf(COLLECT_LIST(concat_ws('|-|',B.timestamp,B.pageUri))) FROM  \
    (SELECT uid FROM adnet_da_report.cogtu_log_view WHERE date = '2015-12-17'  AND reqType = 3 GROUP BY uid) AS A ,  \
    (SELECT * FROM adnet_da_report.cogtu_log_view WHERE date = '2015-12-17' AND reqType = 1 ) AS B WHERE A.uid = B.uid GROUP BY B.uid  " 1 \

##SQL 获取未点击过任务广告的用户的 最近7天的所有访问URL (算法支持用) (工具)
    /usr/local/spark/bin/spark-submit \
    --class spark.sql.SparkSqlQuery \
    --master yarn-client \
    --num-executors 20  \
    --driver-memory 2g \
    --executor-memory 2g --executor-cores 1 /usr/local/spark/adnet-da-report-v2.1.0.jar \
    " \
    SELECT B.uid,regexp_replace(sort_array_timestamp_return_url_array_udf(COLLECT_LIST(concat_ws('|-|',B.timestamp,B.pageUri))),'\\[|\\]','') FROM (SELECT uid FROM adnet_da_report.cogtu_log_view WHERE date<'2015-12-25'  AND reqType = 3 GROUP BY uid) AS A RIGHT OUTER JOIN  (SELECT uid,pageUri,timestamp FROM adnet_da_report.cogtu_log_view WHERE date<'2015-12-25'  AND reqType = 1 ) AS B ON A.uid = B.uid GROUP BY B.uid " 1 \

##SQL URL PV 排序  (临时需求)
    /usr/local/spark/bin/spark-submit \
    --class spark.sql.SparkSqlQuery \
    --master yarn-client \
    --num-executors 20  \
    --driver-memory 2g \
    --executor-memory 2g --executor-cores 1 /usr/local/spark/adnet-da-report-v2.1.0.jar \
    "SELECT url,SUM(1) AS pv FROM (SELECT split(pageUri,'\\\\?')[0] AS url FROM  adnet_da_report.cogtu_log_view WHERE reqType=1 AND date = '2015-12-23'  ) AS A GROUP BY url  ORDER BY pv DESC" 1 \

##HIVE JDBC 启动
    hive --service hiveserver

##HIVE 表
    CREATE DATABASE adnet_da_report;
    CREATE TABLE adnet_da_report.cogtu_log (log STRING) PARTITIONED BY (date date);
    CREATE TABLE adnet_da_report.cogtu_log_split (ip STRING,timestamp BIGINT,slotId STRING,siteId INT,publisherId INT,ref STRING,country STRING,province STRING,city STRING,addrCode STRING,browser STRING,os STRING,uid STRING,runTime BIGINT,vBalanceCostPrecent INT,v STRING,sessionId STRING,source STRING,agent STRING,pageUri STRING,reqType INT,impId STRING,impUrl STRING,advertisersId INT,projectId INT,campaignId INT,creativeId INT,templateId INT,price BIGINT,priceType STRING,features STRING,tags STRING) PARTITIONED BY (date date,hour INT)  ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'  ;
    ALTER TABLE adnet_da_report.cogtu_log_split ADD COLUMNS (campaignType INT);

##HIVE UDF
    ADD JAR /usr/local/spark/adnet-da-report-v2.1.0.jar;
    CREATE TEMPORARY FUNCTION reqs_parse_array_udf AS 'hive.udf.ReqsParseArrayListUdf';
    CREATE TEMPORARY FUNCTION sort_array_timestamp_return_url_array_udf AS 'hive.udf.SortArrayTimeStampReturnUrlArrayUdf';

##HIVE 视图
    DROP VIEW adnet_da_report.cogtu_log_view;
    CREATE VIEW adnet_da_report.cogtu_log_view AS SELECT cl.*,t3.*,from_unixtime(cast(substr(cl.timestamp,0,10) AS INT),'HH') as hour FROM (SELECT t1.*,date,log FROM adnet_da_report.cogtu_log LATERAL VIEW JSON_TUPLE(log,'ip','timestamp','slotId','siteId','publisherId','pageUri','ref','country','province','city','addrCode','browser','os','uid','runTime','vBalanceCostPrecent','v','sessionId','source','reqs','reqType','agent') t1 AS ip,timestamp,slotId,siteId,publisherId,pageUri,ref,country,province,city,addrCode,browser,os,uid,runTime,vBalanceCostPrecent,v,sessionId,source,reqs,reqType,agent) AS cl LATERAL VIEW EXPLODE(reqs_parse_array_udf(reqs)) t2 as arrjson LATERAL VIEW json_tuple(arrjson,"impId","advertisersId","projectId","campaignId","creativeId","templateId","price","priceType","campaignType") t3 as impId,advertisersId,projectId,campaignId,creativeId,templateId,price,priceType,campaignType;
