
CREATE DATABASE IF NOT EXISTS `offline_basic_analysis` CHARACTER SET utf8 COLLATE utf8_general_ci;

SET FOREIGN_KEY_CHECKS=0;

use `offline_basic_analysis`;

-- ----------------------------
-- Table structure for user_db_location
-- ----------------------------
-- DROP TABLE IF EXISTS `user_db_location`;
CREATE TABLE IF NOT EXISTS `user_db_location` (
  `userid` int(11) DEFAULT NULL COMMENT '用户ID',
  `user_type` varchar(255) DEFAULT NULL COMMENT '用户类型',
  `ip` varchar(255) DEFAULT NULL COMMENT '数据库地址',
  `port` varchar(255) DEFAULT NULL COMMENT '数据库端口',
  `username` varchar(255) DEFAULT NULL COMMENT '数据库用户名',
  `password` varchar(255) DEFAULT NULL COMMENT '数据库密码',
  `dbname` varchar(255) DEFAULT NULL COMMENT '数据库名称',
) ENGINE=MyISAM DEFAULT CHARSET=utf8;

-- ----------------------------
-- Table structure for user_table_location
-- ----------------------------
-- DROP TABLE IF EXISTS `user_table_location`;
CREATE TABLE IF NOT EXISTS `user_table_location` (
  `user_type` varchar(255) DEFAULT NULL COMMENT '用户类型',
  `table_name` varchar(255) DEFAULT NULL COMMENT '表名称',
  `dbname` varchar(255) DEFAULT NULL COMMENT '数据库名称'
) ENGINE=MyISAM DEFAULT CHARSET=utf8;

-- ----------------------------
-- Table structure for `admin_all_advertisers_cost_report`
-- ----------------------------
-- DROP TABLE IF EXISTS `admin_all_advertisers_cost_report`;
CREATE TABLE IF NOT EXISTS `admin_all_advertisers_cost_report` (
  `day` date DEFAULT NULL COMMENT '日期',
  `hour` int(11) DEFAULT NULL COMMENT '小时',
  `advertisers_id` int(11) DEFAULT NULL COMMENT '广告主ID',
  `project_id` int(11) DEFAULT NULL COMMENT '广告项目ID',
  `campaign_id` int(11) DEFAULT NULL COMMENT '广告计划',
  `creative_id` int(11) DEFAULT NULL COMMENT '创意ID',
  `amount` bigint(20) DEFAULT NULL COMMENT '金额',
  `v_amount` bigint(20) DEFAULT NULL COMMENT '虚拟金额',
  `complete_folder_name` varchar(255) DEFAULT NULL COMMENT '完成日期标示'
) ENGINE=MyISAM DEFAULT CHARSET=utf8;

CREATE TABLE IF NOT EXISTS `streaming` (
  `name` varchar(255) DEFAULT NULL COMMENT '实时分析服务名称',
  `status` varchar(255) DEFAULT NULL COMMENT '值为stop时会关闭实时分析'
) ENGINE=InnoDB DEFAULT CHARSET=utf8;


CREATE TABLE IF NOT EXISTS `merge_log_list` (
  `day` varchar(255) DEFAULT NULL COMMENT '日期',
  `status` varchar(255) DEFAULT NULL COMMENT '状态Y=已合并 N=未合并',
  `note` varchar(255) DEFAULT NULL  COMMENT '备注'
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE IF NOT EXISTS `merge_table_list` (
  `day` varchar(255) DEFAULT NULL COMMENT '日期',
  `status` varchar(255) DEFAULT NULL COMMENT '状态Y=已合并 N=未合并',
  `user_id` varchar(11) DEFAULT NULL COMMENT '用户ID',
  `user_type` varchar(255) DEFAULT NULL COMMENT '用户类型',
  `table_name` varchar(255) DEFAULT NULL COMMENT '需要合并表的名称',
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '任务ID',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=182 DEFAULT CHARSET=utf8;

CREATE TABLE IF NOT EXISTS `filter_log_rule` (
  `rule` varchar(255) DEFAULT NULL COMMENT '过滤关键字',
) ENGINE=InnoDB DEFAULT CHARSET=utf8;


CREATE TABLE IF NOT EXISTS `customize_report_info` (
  `dimension` varchar(255) DEFAULT '' COMMENT '维度',
  `index` varchar(255) DEFAULT '' COMMENT '指标',
  `status` varchar(255) DEFAULT '' COMMENT 'N 未开始分析,I 正在分析中,Y 分析完毕',
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '任务ID',
  `condition` varchar(255) DEFAULT '' COMMENT '条件',
  `display` varchar(255) DEFAULT '' COMMENT '显示字段',
  `creat_time` datetime DEFAULT NULL COMMENT '创建日期',
  `update_time` datetime DEFAULT NULL COMMENT '更新日期',
  `table_name` varchar(255) DEFAULT '' COMMENT '查询表',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=7 DEFAULT CHARSET=utf8;

CREATE TABLE IF NOT EXISTS `customize_report_option` (
  `name` varchar(255) DEFAULT NULL COMMENT '名称',
  `type` varchar(255) DEFAULT NULL COMMENT '类型',
  `description` varchar(255) DEFAULT NULL COMMENT '描述'
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

