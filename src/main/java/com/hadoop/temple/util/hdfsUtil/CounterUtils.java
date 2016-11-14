package com.hadoop.temple.util.hdfsUtil;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import com.hadoop.temple.util.*;

/**
 * 计数器帮助类 User: 马明 Date: 2015-07-15
 */
public class CounterUtils {

	/**
	 * 向默认的文件中输出counter信息。 目录格式： Constants.OUT_PATH_GROUP_COUNT / 日期
	 * /GroupName.txt
	 * 
	 * @param counterGroup
	 *            组名
	 * @param conf
	 *            配置文件
	 */
	public static void writeToDefaultFile(CounterGroup counterGroup,
			Configuration conf) {
		String path = StringUtil.formatMessage(Constants.OUT_PATH_GROUP_COUNT,
				conf.get(Constants.HANDLE_WHICH_DAY), counterGroup.getName());
		Path counterOutPath = new Path(path);
		writeTo(counterGroup, conf, counterOutPath);
	}

	/**
	 * 向默认的文件中输出counter信息。 目录格式
	 * 
	 * @param counterGroup
	 *            组名
	 * @param conf
	 *            配置文件
	 */
	public static void writeTo(CounterGroup counterGroup, Configuration conf,
			Path outPath) {
		BufferedWriter out = null;
		try {
			FileSystem fs = FileSystem.get(conf);
			FSDataOutputStream fout = null;
			if (fs.exists(outPath)) {
				// fout = fs.append(outPath);
				fs.delete(outPath, true);
			}
			fout = fs.create(outPath, false);

			out = new BufferedWriter(new OutputStreamWriter(fout, "UTF-8"));
			out.write(counterGroup.getDisplayName());
			out.newLine();
			for (Counter counter : counterGroup) {
				out.write(counter.getDisplayName() + ":" + counter.getValue());
				out.newLine();
			}
			out.flush();
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (null != out) {
				try {
					out.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}
}
