package com.hadoop.temple.util.hdfsUtil;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.logging.log4j.Logger;
import com.hadoop.temple.util.*;

public class HdfsUtil {

	private static Logger LOG = LogWriter.getOther();
	private static Logger ERROR_LOG = LogWriter.getErrorLog();

	static Configuration conf = new Configuration();

	private static FileSystem hdfs = null;

	public static FileSystem getFileSystem() {
		if (hdfs != null) {
			return hdfs;
		}
		// conf.set("fs.default.name", Constants.HDFS_URL);//Hadoop1.0key
		conf.set("fs.defaultFS", Constants.HDFS_URL);// Hadoop2.0key
		try {
			hdfs = FileSystem.get(conf);
			LOG.info("HdfsUtil getFileSystem init success!");
		} catch (IOException e) {
			ERROR_LOG.info("HdfsUtil getFileSystem error!", e);
		}
		return hdfs;
	}
}
