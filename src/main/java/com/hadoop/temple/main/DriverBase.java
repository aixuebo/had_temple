package com.hadoop.temple.main;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileAlreadyExistsException;
import org.apache.logging.log4j.Logger;

import com.hadoop.temple.pojo.Command;
import com.hadoop.temple.util.LogWriter;


/**
 * 任务驱动基类 User: 马明 Date: 2015-07-15
 */
public abstract class DriverBase extends Configured {

	private static Logger LOG = LogWriter.getOther();

	/**
	 * 任务执行结果
	 */
	private Result result;

	/**
	 * 任务执行所需要的参数信息
	 */
	private Command command;

	/**
	 * 执行任务
	 * 
	 * @param params
	 *            任务所需要的参数
	 * @throws IOException
	 */
	public void drive(Command command) {
		this.command = command;
		try {
			result = this.action(command);
			// this.afterDrive();
		} catch (FileAlreadyExistsException faee) {
			LOG.error("", faee.getMessage());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * 子类需要实现的动作
	 * 
	 * @param params
	 *            参数
	 * @return Result 任务执行结果
	 */
	protected abstract Result action(Command command) throws Exception;

	/**
	 * 驱动执行后执行的动作。 默认实现是检测任务是否执行成功，如果失败则重新执行.
	 */
	public void afterDrive() throws Exception {
		if (null == result || ((null != result) && "".equals(result.getPath()))) {
			return;
		}
		FileSystem fs = FileSystem.get(getConf());
		Path targetPath = new Path(result.getPath());

		boolean isRepeat = false;// 是否重复执行
		for (int i = 0; i < 3; i++) {// 执行失败，尝试3次
			if (!result.isSuccess() || isAllOutputFileEmpty(fs, targetPath)) {
				if (fs.exists(targetPath)) {
					fs.delete(targetPath, true);
				}
				isRepeat = true;// 重复执行
				// EmailUtils.sendSimpleTextMessage("re-execution task, output path: "
				// + result.getPath());
				result = this.action(command);
			} else {
				if (isRepeat) {// 如果重复执行,并且成功执行,则发邮件
					// EmailUtils.sendSimpleTextMessage("re-execution task [success], output path: "
					// +
					// result.getPath());
				}
			}
		}

		// 发送邮件通知，手动回复,因为重复三次还不成功
		if (!result.isSuccess() || isAllOutputFileEmpty(fs, targetPath)) {
			// EmailUtils.sendSimpleTextMessage("re-execution task [fail], output path: "
			// +
			// result.getPath());
		}

	}

	/**
	 * 检测输出目录下，所有文件是否为空
	 * 
	 * @param fs
	 *            hadoop文件系统
	 * @param parentPath
	 *            输出目录
	 * @return boolean true表示为空，否则为false。
	 * @throws Exception
	 */
	private boolean isAllOutputFileEmpty(FileSystem fs, Path parentPath)
			throws Exception {
		parentPath = new Path(parentPath, "part*");
		FileStatus[] fileStatuses = fs.globStatus(parentPath);
		return fileStatuses != null && fileStatuses.length > 0;
	}
}
