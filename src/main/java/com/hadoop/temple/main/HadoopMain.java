package com.hadoop.temple.main;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.logging.log4j.Logger;

import com.hadoop.temple.job.TextToVectorSequenceFileDriver;
import com.hadoop.temple.pojo.Command;
import com.hadoop.temple.util.LogWriter;


/**
 * 系统入口类 User: 马明 Date: 2015-07-15
 */
public class HadoopMain extends Configured implements Tool {

	private static Logger LOG = LogWriter.getOther();

	public static void main(String args[]) throws Exception {
		Configuration conf = new Configuration();
		// conf.set("mapreduce.job.queuename", "statistic");
		/*
		 * conf.set("mapreduce.map.failures.maxpercent", "10");//设置允许失败的task百分比
		 * conf.set("mapreduce.map.maxattempts", "1");//设置尝试此时为1即可
		 */
		int res = ToolRunner.run(conf, new HadoopMain(), args);
		System.exit(res);
	}

	private Command command;

	@Override
	public int run(String[] args) throws Exception {
		try {
			LOG.info(" HadoopMain begin");
			command = new Command(args);

			// 执行测试任务
			if (command.getTask().equals("test")) {
				
				TextToVectorSequenceFileDriver driver = new TextToVectorSequenceFileDriver(this.getConf());
				driver.drive(this.getCommand());
				LOG.info("TextToVectorSequenceFileDriver ending");
				
				return 0;
			}

			// 执行正常的任务
			String[] taskList = command.getTask().equals("all") ? TaskRunnable.TASK_GROUP_NAMES
					: command.getTask().split(",");
			LOG.info("task:"+command.getTask());
			for (String task : taskList) {
                TaskRunnable.runTask(this, task);
            }
			LOG.info(" HadoopMain endding");
		} catch (Exception ex) {
			LOG.info("HadoopMain run error!", ex);
		}
		return 0;
	}

	public Command getCommand() {
		return command;
	}

	public void setCommand(Command command) {
		this.command = command;
	}

}
