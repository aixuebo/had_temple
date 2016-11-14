package com.hadoop.temple.main;

/**
 * 任务执行结果 User: 马明 Date: 2015-07-15
 */
public class Result {

	/**
	 * 任务执行是否成功 true：表示成功 false：表示失败
	 */
	private boolean success = false;

	/**
	 * 结果输出目录
	 */
	private String path = "";

	public Result(boolean success, String outPath) {
		this.success = success;
		this.path = outPath;
	}

	public boolean isSuccess() {
		return success;
	}

	public void setSuccess(boolean success) {
		this.success = success;
	}

	public String getPath() {
		return path;
	}

	public void setPath(String path) {
		this.path = path;
	}
}
