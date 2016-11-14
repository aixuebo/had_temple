package com.hadoop.temple.util.hdfsUtil;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.logging.log4j.Logger;

import com.hadoop.temple.util.*;


public class DistriebuteCacheUtil extends Configured {

	private static Logger LOG = LogWriter.getOther();
	private static Logger ERROR_LOG = LogWriter.getErrorLog();

	/***
	 * 缓存是目录
	 * 
	 * @param job
	 * @param fs
	 * @param path
	 *            是支持{}、*的通配符
	 */
	public static void saveCacheDirectory(Job job, FileSystem fs, Path path) {
		try {
			FileStatus[] fileList = fs.globStatus(path);
			for (FileStatus file : fileList) {
				Path targetPath = file.getPath();
				job.addCacheFile(targetPath.toUri());
			}
		} catch (IOException e) {
			ERROR_LOG.error("DistriebuteCacheUtil init error!", e);
		}
	}

	/**
	 * 仅缓存一个文件
	 * 
	 * @param path
	 *            文件的具体路径
	 */
	public static void saveCacheFile(Job job, Path path) throws IOException {
		job.addCacheFile(path.toUri());
		LOG.info("Cache file :{} ", path.toString());
	}

}
