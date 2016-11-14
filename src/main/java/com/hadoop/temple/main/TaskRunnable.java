package com.hadoop.temple.main;

import org.apache.logging.log4j.Logger;

import com.hadoop.temple.job.SequenceFileToTextDriver;
import com.hadoop.temple.job.TextToVectorSequenceFileDriver;
import com.hadoop.temple.util.LogWriter;
import com.hadoop.temple.util.StringUtil;

public class TaskRunnable {

	// 任务组,一组任务同时执行
	public final static String[] TASK_GROUP_NAMES = {""};

	/**
	 * 小任务,非分组的任务
	 */
	public final static String[] SMALL_TASK_NAMES = { "textToVector","sequenceTotext"};
			

	private static Logger LOG = LogWriter.getOther();
	private static Logger ERROR_LOG = LogWriter.getErrorLog();

	public static void runTask(HadoopMain hadoopMain, String task) {
		try {
			if ("".equals(StringUtil.trim(task))) {
				return;
			}
			if ("textToVector".equals(task)) {
				TextToVectorSequenceFileDriver driver = new TextToVectorSequenceFileDriver(hadoopMain.getConf());
				driver.drive(hadoopMain.getCommand());
				LOG.info("TextToVectorSequenceFileDriver ending");
			} else if ("sequenceTotext".equals(task)) {
				SequenceFileToTextDriver driver = new SequenceFileToTextDriver(hadoopMain.getConf());
				driver.drive(hadoopMain.getCommand());
				LOG.info("SequenceFileToTextDriver ending");
			}
		} catch (Exception ex) {
			ERROR_LOG.info("TaskRunnable runTask is error! task is :", task, ex);
		}
	}
}
