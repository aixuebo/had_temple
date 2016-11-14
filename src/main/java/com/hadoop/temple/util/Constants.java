package com.hadoop.temple.util;

public interface Constants {

	// 处理那一天的日志
	public static final String HANDLE_WHICH_DAY = "log.date";

	public static final String KEY_DELIMITER = "/";
	public static final String VALUE_DELEMITER = "\t";
	public static final String VALUE_HIVE_DELEMITER = "\\001";

	// hadoop目录
	public static final String HDFS_URL = "hdfs://namenode1:9000";// fs.default.name
	
	public static final String OUT_PATH_GROUP_COUNT = "/log/statistics/groupcount/{0}/{1}.txt";//0表示时间 1表示文件名 

	public static final int BUFFER_SIZE = 8 * 1024 * 1024;// 8M缓存
}
