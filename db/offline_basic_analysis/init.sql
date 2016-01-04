
use `offline_basic_analysis`;



-- ----------------------------
-- 用户对应所属报表
-- ----------------------------
INSERT INTO `user_table_location` VALUES ('advertisers', 'advertisers_area_report', 'offline_basic_analysis');
INSERT INTO `user_table_location` VALUES ('advertisers', 'advertisers_project_report', 'offline_basic_analysis');
INSERT INTO `user_table_location` VALUES ('admin', 'admin_all_advertisers_cost_report', 'offline_basic_analysis');
INSERT INTO `user_table_location` VALUES ('admin', 'admin_all_day_report', 'offline_basic_analysis');
INSERT INTO `user_table_location` VALUES ('admin', 'admin_all_project_report', 'offline_basic_analysis');
INSERT INTO `user_table_location` VALUES ('admin', 'admin_feature_report', 'offline_keyword_feature_image_analysis');
INSERT INTO `user_table_location` VALUES ('admin', 'admin_image_report', 'offline_keyword_feature_image_analysis');
INSERT INTO `user_table_location` VALUES ('admin', 'admin_keyword_report', 'offline_keyword_feature_image_analysis');
INSERT INTO `user_table_location` VALUES ('publisher', 'publisher_income_report', 'offline_basic_analysis');
INSERT INTO `user_table_location` VALUES ('publisher', 'publisher_creative_report', 'offline_basic_analysis');
INSERT INTO `user_table_location` VALUES ('publisher', 'publisher_slot_report', 'offline_basic_analysis');

-- ----------------------------
-- 用户数据库信息
-- ----------------------------
INSERT INTO `user_db_location` VALUES ('-1', 'admin', 'mysql01', '3306', 'root', 'tusimple@media', 'offline_basic_analysis');
INSERT INTO `user_db_location` VALUES ('-1', 'admin', 'mysql01', '3306', 'root', 'tusimple@media', 'offline_keyword_feature_image_analysis');
INSERT INTO `user_db_location` VALUES ('1', 'publisher', 'mysql01', '3306', 'root', 'tusimple@media', 'offline_basic_analysis');

-- ----------------------------
-- 实时分析的关闭开关
-- ----------------------------
INSERT INTO `streaming` VALUES ('kafka_to_hdfs', '');

-- ----------------------------
-- 过滤信息
-- ----------------------------
INSERT INTO `filter_log_rule` VALUES ('QQ-URL-Manager');
INSERT INTO `filter_log_rule` VALUES ('QQ-Manager');
INSERT INTO `filter_log_rule` VALUES ('\"reqtype\":-1');

-- ----------------------------
-- 增加管理员地域报表库信息
-- ----------------------------
INSERT INTO offline_basic_analysis.`user_table_location` VALUES ('admin', 'admin_all_area_report', 'offline_basic_analysis');

-- ----------------------------
-- 自定义报表定义字典
-- ----------------------------

-- ----------------------------
-- Records of customize_report_option
-- ----------------------------
INSERT INTO `customize_report_option` VALUES ('date', 'filter', '日期');
INSERT INTO `customize_report_option` VALUES ('slotId', 'filter', '广告位ID');
INSERT INTO `customize_report_option` VALUES ('siteId', 'filter', '网站ID');
INSERT INTO `customize_report_option` VALUES ('publisherId', 'filter', '网站主ID');
INSERT INTO `customize_report_option` VALUES ('ref', 'filter', '来源URL');
INSERT INTO `customize_report_option` VALUES ('country', 'filter', '国家');
INSERT INTO `customize_report_option` VALUES ('province', 'filter', '省');
INSERT INTO `customize_report_option` VALUES ('city', 'filter', '城市');
INSERT INTO `customize_report_option` VALUES ('browser', 'filter', '浏览器');
INSERT INTO `customize_report_option` VALUES ('os', 'filter', '操作系统');
INSERT INTO `customize_report_option` VALUES ('impId', 'filter', '图片ID');
INSERT INTO `customize_report_option` VALUES ('impUrl', 'filter', '图片URL');
INSERT INTO `customize_report_option` VALUES ('projectId', 'filter', '广告项目');
INSERT INTO `customize_report_option` VALUES ('campaignId', 'filter', '广告计划');
INSERT INTO `customize_report_option` VALUES ('creativeId', 'filter', '广告创意');
INSERT INTO `customize_report_option` VALUES ('features', 'filter', '图片特征');
INSERT INTO `customize_report_option` VALUES ('tags', 'filter', '图片标签');
INSERT INTO `customize_report_option` VALUES ('fetch_', 'index', '接口请求数量');
INSERT INTO `customize_report_option` VALUES ('req', 'index', '广告请求数量');
INSERT INTO `customize_report_option` VALUES ('imp', 'index', '广告展示量');
INSERT INTO `customize_report_option` VALUES ('click', 'index', '广告点击量');
INSERT INTO `customize_report_option` VALUES ('price', 'index', '价格');
INSERT INTO `customize_report_option` VALUES ('filling', 'index', '填充量');
INSERT INTO `customize_report_option` VALUES ('unique_ip', 'index', '独立IP量');
INSERT INTO `customize_report_option` VALUES ('unique_uid', 'index', '独立用户数');
INSERT INTO `customize_report_option` VALUES ('hour', 'dimension', '时段');
INSERT INTO `customize_report_option` VALUES ('date', 'dimension', '日期');
INSERT INTO `customize_report_option` VALUES ('slotId', 'dimension', '广告位ID');
INSERT INTO `customize_report_option` VALUES ('siteId', 'dimension', '网站ID');
INSERT INTO `customize_report_option` VALUES ('publisherId', 'dimension', '网站主ID');
INSERT INTO `customize_report_option` VALUES ('ref', 'dimension', '来源URL');
INSERT INTO `customize_report_option` VALUES ('country', 'dimension', '国家');
INSERT INTO `customize_report_option` VALUES ('province', 'dimension', '省');
INSERT INTO `customize_report_option` VALUES ('city', 'dimension', '城市');
INSERT INTO `customize_report_option` VALUES ('browser', 'dimension', '浏览器');
INSERT INTO `customize_report_option` VALUES ('os', 'dimension', '操作系统');
INSERT INTO `customize_report_option` VALUES ('impId', 'dimension', '图片ID');
INSERT INTO `customize_report_option` VALUES ('impUrl', 'dimension', '图片URL');
INSERT INTO `customize_report_option` VALUES ('projectId', 'dimension', '广告项目');
INSERT INTO `customize_report_option` VALUES ('campaignId', 'dimension', '广告计划');
INSERT INTO `customize_report_option` VALUES ('creativeId', 'dimension', '广告创意');
INSERT INTO `customize_report_option` VALUES ('features', 'dimension', '图片特征');
INSERT INTO `customize_report_option` VALUES ('tags', 'dimension', '图片标签');
INSERT INTO `customize_report_option` VALUES ('sessionId', 'filter', '会话ID');
