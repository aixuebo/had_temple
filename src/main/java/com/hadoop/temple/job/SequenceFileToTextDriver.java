package com.hadoop.temple.job;

import java.io.IOException;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.logging.log4j.Logger;
import org.apache.mahout.math.VectorWritable;

import com.hadoop.temple.main.CounterName;
import com.hadoop.temple.main.DriverBase;
import com.hadoop.temple.main.Result;
import com.hadoop.temple.pojo.Command;
import com.hadoop.temple.util.Constants;
import com.hadoop.temple.util.DateUtils;
import com.hadoop.temple.util.LogWriter;
import com.hadoop.temple.util.hdfsUtil.CounterUtils;
/**
 * SequenceFile形式,并且value是mahout的Vector对象,转换成Text文本
 */
public class SequenceFileToTextDriver extends DriverBase {

	private static Logger LOG = LogWriter.getOther();

	public SequenceFileToTextDriver(Configuration conf) {
		setConf(conf);
	}

	public static class InterestRecordMapper extends
			Mapper<Text, VectorWritable, Text, Text> {
		
		@Override
		protected void map(Text key, VectorWritable values, Context context)
				throws IOException, InterruptedException {

			context.getCounter(CounterName.SequenceFileToTextDriver.NAME,
					CounterName.SequenceFileToTextDriver.RECORD_TOTAL_MAPPER).increment(1);// 多少行日志
			
			try {
				String line = values.get().toString();
				context.write(key, new Text(line));
			} catch (Exception ex) {
				context.getCounter(CounterName.SequenceFileToTextDriver.NAME,
						CounterName.SequenceFileToTextDriver.HANDLE_RECORD_FAIL_TOTAL_MAPPER)
						.increment(1);
			}
		}
	}

	protected Result action(Command command) throws Exception {
		String jobName = SequenceFileToTextDriver.class.getSimpleName();
		String logDate = command.getDate();
		getConf().set(Constants.HANDLE_WHICH_DAY, logDate);
		getConf().set("vector.implementation.class.name", command.getVectorClassName());
		
		Path inputPath = new Path(command.getInput());
		Path outPath = new Path(command.getOutput());
	    
		long start = System.currentTimeMillis();
		LOG.info(jobName+": start at "
				+ DateUtils.getDefaultDateFormat(new Date(start)));
		LOG.info(jobName + " input directory: " + inputPath.toString());
		LOG.info(jobName + " out directory: " + outPath.toString());
		Job job = Job.getInstance(this.getConf(), jobName + ":" + logDate);
				
		job.setNumReduceTasks(0);
		job.setJarByClass(SequenceFileToTextDriver.class);
		job.setMapperClass(InterestRecordMapper.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outPath);
		FileSystem fs = FileSystem.get(this.getConf());
		fs.delete(outPath, true);

		boolean success = job.waitForCompletion(true);
		long end = System.currentTimeMillis();
		LOG.info(jobName + ": finished at "
				+ DateUtils.getDefaultDateFormat(new Date(end)) + ", elapsed: "
				+ (end - start));
		CounterUtils.writeToDefaultFile(
				job.getCounters()
						.getGroup(CounterName.SequenceFileToTextDriver.NAME), job
						.getConfiguration());
		return new Result(success, outPath.toString());
	}
}
