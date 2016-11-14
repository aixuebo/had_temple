package com.hadoop.temple.pojo;

import java.util.Calendar;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.logging.log4j.Logger;

import com.hadoop.temple.util.*;

public class Command {

	private static Logger LOG = LogWriter.getOther();

	private String task;// 用逗号拆分
	private String date;

	private String input;
	private String output;
	
	
	//mathout
	private String vectorClassName = "org.apache.mahout.math.RandomAccessSparseVector";//默认向量类型
	
	public Command(String[] args) {

		CommandLine commandLine = parseArgs(args);

		this.date = DateUtil.dateConvertSingle(Calendar.DATE, -1);// 格式
																	// yyyyMMdd,默认是前一天日期，即当天-1
		this.task = "";

		if (commandLine.hasOption("d")) {
			this.date = commandLine.getOptionValue("d");
		}

		if (commandLine.hasOption("t")) {
			this.task = commandLine.getOptionValue("t");
		}
		
		if (commandLine.hasOption("input")) {
			this.input = commandLine.getOptionValue("input");
		}
		
		if (commandLine.hasOption("output")) {
			this.output = commandLine.getOptionValue("output");
		}
		
		if (commandLine.hasOption("vectorClassName")) {
			this.vectorClassName = commandLine.getOptionValue("vectorClassName");
		}
	}

	/**
	 * 解析命令参数
	 */
	public CommandLine parseArgs(String[] args) {
		Options options = new Options();
		options.addOption("d", true, "处理的日期");
		options.addOption("t", true, "任务类型");
		options.addOption("input", true, "输入路径");
		options.addOption("output", true, "输出路径");
		options.addOption("vectorClassName", true, "向量类型");
		
		CommandLineParser parser = new PosixParser();
		CommandLine cmd = null;
		try {
			cmd = parser.parse(options, args);
			LOG.info("参数个数为:{}", cmd.getOptions().length);
		} catch (ParseException e) {
			e.printStackTrace();
			return null;
		}
		return cmd;
	}

	public static Logger getLOG() {
		return LOG;
	}

	public String getTask() {
		return task;
	}

	public String getDate() {
		return date;
	}

	public String getInput() {
		return input;
	}

	public String getOutput() {
		return output;
	}

	public String getVectorClassName() {
		return vectorClassName;
	}
	
}
