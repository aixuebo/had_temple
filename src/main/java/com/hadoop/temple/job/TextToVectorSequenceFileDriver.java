package com.hadoop.temple.job;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.logging.log4j.Logger;
import org.apache.mahout.math.Vector;
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
 * Text文本转换成SequenceFile形式,并且value是mahout的Vector对象
 */
public class TextToVectorSequenceFileDriver extends DriverBase {

	private static Logger LOG = LogWriter.getOther();

	public TextToVectorSequenceFileDriver(Configuration conf) {
		setConf(conf);
	}

	public static class InterestRecordMapper extends
			Mapper<LongWritable, Text, Text, VectorWritable> {

		private static final Pattern SPACE = Pattern.compile(" ");

		private Constructor<?> constructor;
		  
		@Override
		protected void map(LongWritable key, Text values, Context context)
				throws IOException, InterruptedException {

			context.getCounter(CounterName.TextToVectorSequenceFileDriverGroup.NAME,
					CounterName.TextToVectorSequenceFileDriverGroup.RECORD_TOTAL_MAPPER).increment(1);// 多少行日志
			String line = values.toString();

			try {
				
				String[] numbers = SPACE.split(line.toString());
			    Collection<Double> doubles = new ArrayList<>();
			    for (String value : numbers) {
			      if (!value.isEmpty()) {
			        doubles.add(Double.valueOf(value));
			      }
			    }
			    // ignore empty lines in data file
			    if (!doubles.isEmpty()) {
			      try {
			        Vector result = (Vector) constructor.newInstance(doubles.size());
			        int index = 0;
			        for (Double d : doubles) {
			          result.set(index++, d);
			        }
			        VectorWritable vectorWritable = new VectorWritable(result);
			        context.write(new Text(String.valueOf(index)), vectorWritable);

			      } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
			        throw new IllegalStateException(e);
			      }
			    }
			} catch (Exception ex) {
				context.getCounter(CounterName.TextToVectorSequenceFileDriverGroup.NAME,
						CounterName.TextToVectorSequenceFileDriverGroup.HANDLE_RECORD_FAIL_TOTAL_MAPPER)
						.increment(1);
			}
		}
		
	  @Override
	  protected void setup(Context context) throws IOException, InterruptedException {
	    super.setup(context);
	    Configuration conf = context.getConfiguration();
	    String vectorImplClassName = conf.get("vector.implementation.class.name");
	    try {
	      Class<? extends Vector> outputClass = conf.getClassByName(vectorImplClassName).asSubclass(Vector.class);
	      constructor = outputClass.getConstructor(int.class);
	    } catch (NoSuchMethodException | ClassNotFoundException e) {
	      throw new IllegalStateException(e);
	    }
	  }
	}

	protected Result action(Command command) throws Exception {
		String jobName = TextToVectorSequenceFileDriver.class.getSimpleName();
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
		job.setJarByClass(TextToVectorSequenceFileDriver.class);
		job.setMapperClass(InterestRecordMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(VectorWritable.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

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
						.getGroup(CounterName.TextToVectorSequenceFileDriverGroup.NAME), job
						.getConfiguration());
		return new Result(success, outPath.toString());
	}
}
