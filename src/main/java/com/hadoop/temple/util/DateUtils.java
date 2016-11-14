package com.hadoop.temple.util;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

/**
 * 日期工具类 User: 马明 Date: 2015-07-17
 */
public class DateUtils {

	/**
	 * 获取前一天的日期，格式为yyyyMMdd
	 * 
	 * @return 日期字符串
	 */
	public static String getPrevDayShortFormatDate() {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		Calendar now = Calendar.getInstance();
		now.add(Calendar.DATE, -1); // 向前推一天
		return sdf.format(now.getTime());
	}

	public static String getPrevDayShortFormatDate(String date) {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		Calendar nowDate = Calendar.getInstance();
		try {
			nowDate.setTime(sdf.parse(date));
		} catch (ParseException e) {
			e.printStackTrace();
		}
		nowDate.add(Calendar.DATE, -1);
		return sdf.format(nowDate.getTime());
	}

	/**
	 * 日期向前推几天
	 * 
	 * @param dateFormat
	 *            参照点
	 * @param prev
	 *            向前推几天
	 */
	public static String getPrevDay(String dateFormat, int prev) {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		try {
			Date date = sdf.parse(dateFormat);
			Calendar calendar = Calendar.getInstance();
			calendar.setTime(date);
			calendar.add(Calendar.DATE, -1); // 向前推一天
			return sdf.format(calendar.getTime());
		} catch (ParseException e) {
			e.printStackTrace();
			return dateFormat;
		}
	}

	/**
	 * 当前日期的前一天
	 */
	public static String getPrevDay() {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		Calendar calendar = Calendar.getInstance();
		calendar.add(Calendar.DATE, -1); // 向前推一天
		return sdf.format(calendar.getTime());
	}

	/**
	 * 获取年份
	 */
	public static String getYearOfPrevDay() {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy");
		Calendar calendar = Calendar.getInstance();
		calendar.add(Calendar.DATE, -1); // 向前推一天
		return sdf.format(calendar.getTime());
	}

	/**
	 * 日期向前推几天
	 * 
	 * @param dateFormat
	 *            参照点
	 * @param back
	 *            向前推几天
	 */
	public static String getBackDay(String dateFormat, int back) {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		try {
			Date date = sdf.parse(dateFormat);
			Calendar calendar = Calendar.getInstance();
			calendar.setTime(date);
			calendar.add(Calendar.DATE, 1);
			return sdf.format(calendar.getTime());
		} catch (ParseException e) {
			e.printStackTrace();
			return dateFormat;
		}
	}

	/**
	 * 获取特定时间的日期格式（yyyyMMdd）
	 * 
	 * @param time
	 *            日期
	 * @return 日期字符串
	 */
	public static String getShortFormatDate(long time) {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		Date date = new Date();
		date.setTime(time);
		return sdf.format(date);
	}

	/**
	 * 获取特定时间的日期格式（HH）
	 * 
	 * @param time
	 *            日期
	 * @return 小时字符串
	 */
	public static String getHourOfDate(long time) {
		SimpleDateFormat sdf = new SimpleDateFormat("HH");
		Date date = new Date();
		date.setTime(time);
		return sdf.format(date);
	}

	/**
	 * 获取当前的日期，格式为yyyyMMdd
	 * 
	 * @return 日期字符串
	 */
	public static String getCurrentDayShortFormatDate() {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		Calendar now = Calendar.getInstance();
		return sdf.format(now.getTime());
	}

	/**
	 * 获取当前时间的前一小时，格式为HH
	 * 
	 * @return 日期字符串
	 */
	public static String getPrevOneHourOfNow() {
		SimpleDateFormat sdf = new SimpleDateFormat("HH");
		Calendar now = Calendar.getInstance();
		// SimpleDateFormat sdf2 = new SimpleDateFormat("yyyyMMdd HH");
		// try {
		// now.setTime(sdf2.parse("20130408 00"));
		// } catch (ParseException e) {
		// e.printStackTrace();
		// }
		now.add(Calendar.HOUR_OF_DAY, -1); // 向前推一小时
		return sdf.format(now.getTime());
	}

	/**
	 * 获取指定时间的前一小时
	 * 
	 * @param datetime
	 *            时间 (yyyyMMddHH)
	 * @return String
	 */
	public static String getPrevOneHourOf(String datetime) {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHH");
		Calendar specialDate = Calendar.getInstance();
		try {
			specialDate.setTime(sdf.parse(datetime));
		} catch (ParseException e) {
			e.printStackTrace();
		}
		specialDate.add(Calendar.HOUR_OF_DAY, -1); // 向前推一小时
		return sdf.format(specialDate.getTime());
	}

	/**
	 * 向前移动几小时。
	 * 
	 * @param targetHour
	 *            参照点
	 * @param prev
	 *            向前移动的小时数
	 */
	public static String getPrevHour(int targetHour, int prev) {
		SimpleDateFormat sdf = new SimpleDateFormat("HH");
		Calendar now = Calendar.getInstance();
		now.set(Calendar.HOUR_OF_DAY, targetHour);
		now.add(Calendar.HOUR_OF_DAY, -prev);
		return sdf.format(now.getTime());
	}

	/**
	 * 向后移动几小时。
	 * 
	 * @param targetHour
	 *            参照点
	 * @param back
	 *            向后移动的小时数
	 */
	public static String getBackHour(int targetHour, int back) {
		SimpleDateFormat sdf = new SimpleDateFormat("HH");
		Calendar now = Calendar.getInstance();
		now.set(Calendar.HOUR_OF_DAY, targetHour);
		now.add(Calendar.HOUR_OF_DAY, back);
		return sdf.format(now.getTime());
	}

	/**
	 * 获取当前小时，格式为HH
	 * 
	 * @return 日期字符串
	 */
	public static String getCurrentHour() {
		SimpleDateFormat sdf = new SimpleDateFormat("HH");
		Calendar now = Calendar.getInstance();
		return sdf.format(now.getTime());
	}

	/**
	 * 返回上一个月实际有多少天
	 */
	public static int getDayOfPreMonthNum() {
		Calendar now = Calendar.getInstance();
		// 当是1月份时，减一是不是上一年的12月呢？
		now.set(Calendar.MONTH, now.get(Calendar.MONTH) - 1); // 向前推一月
		return now.getActualMaximum(Calendar.DATE);
	}

	/**
	 * 返回一个月有多少天
	 * 
	 * @param month
	 *            月份
	 * @return int
	 */
	public static int getDayNumOf(int month) {
		if ((month < 1) || (month > 12)) {
			return 0;
		}
		Calendar date = Calendar.getInstance();
		date.set(Calendar.MONTH, month - 1);
		return date.getActualMaximum(Calendar.DATE);
	}

	/**
	 * 获取月份 (yyyyMMdd)
	 * 
	 * @param dateString
	 *            日期
	 */
	public static int getMonth(String dateString) {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		try {
			sdf.setLenient(false);
			Date date = sdf.parse(dateString);
			Calendar calendar = Calendar.getInstance();
			calendar.setTime(date);
			return calendar.get(Calendar.MONTH) + 1;
		} catch (ParseException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * 获取那一天 (yyyyMMdd)
	 * 
	 * @param dateString
	 *            日期r
	 */
	public static int getDay(String dateString) {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		try {
			sdf.setLenient(false);
			Date date = sdf.parse(dateString);
			Calendar calendar = Calendar.getInstance();
			calendar.setTime(date);
			return calendar.get(Calendar.DAY_OF_MONTH);
		} catch (ParseException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * 获取年份和月份【yyyyMM】
	 */
	public static String getPreMonthShortFormatDate() {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMM");
		Calendar now = Calendar.getInstance();
		now.add(Calendar.MONTH, -1); // 向前推一天
		return sdf.format(now.getTime());
	}

	/**
	 * 获取系统日期，格式为：yyyy-MM-dd HH:mm:ss
	 * 
	 * @return 日期字符串
	 */
	public static String getCurrentDate() {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Date currentDate = new Date();
		return sdf.format(currentDate);
	}

	/**
	 * 获取系统日期，格式为：yyyyMMdd
	 * 
	 * @return 日期字符串
	 */
	public static String getCurrentShortDateFormat() {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		Date currentDate = new Date();
		return sdf.format(currentDate);
	}

	/**
	 * 格式化日志格式为：yyyy-MM-dd HH:mm:ss
	 * 
	 * @param date
	 *            被格式化的日志
	 * @return 日期字符串
	 */
	public static String getDefaultDateFormat(Date date) {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		return sdf.format(date);
	}

	/**
	 * 获取一年中的某一个月中的日期字符串列表 （yyyyMMdd）
	 * 
	 * @param year
	 *            年
	 * @param month
	 *            月
	 */
	public static List<String> getDateStringsOf(int year, int month) {
		List<String> dateList = new ArrayList<String>();
		StringBuilder dateBuilder = new StringBuilder();
		dateBuilder.append(year);
		if (month < 10) {
			dateBuilder.append("0").append(month);
		} else {
			dateBuilder.append(month);
		}
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMM");
		sdf.setLenient(false);
		Date date = null;
		try {
			date = sdf.parse(dateBuilder.toString());
		} catch (ParseException e) {
			throw new RuntimeException(e);
		}
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(date);
		int maxDayNum = calendar.getActualMaximum(Calendar.DATE);
		String prefixDate = dateBuilder.toString();
		for (int i = 1; i <= maxDayNum; i++) {
			if (i < 10) {
				dateList.add(prefixDate + "0" + i);
			} else {
				dateList.add(prefixDate + i);
			}
		}
		return dateList;
	}

	/**
	 * 返回一个日期列表 (yyyyMMdd)
	 * 
	 * @param startDate
	 *            开始日期（包含）
	 * @param endDate
	 *            结束日志（包含）
	 * @return list
	 */
	public static List<String> getDateStrings(String startDate, String endDate) {
		List<String> dateList = new ArrayList<String>();
		if (!DateUtils.validateDateFormat(startDate)
				|| !DateUtils.validateDateFormat(endDate)) {
			return new ArrayList<String>();
		}
		int startYear = Integer.parseInt(startDate.substring(0, 4));
		int endYear = Integer.parseInt(endDate.substring(0, 4));
		if (startYear > endYear) {
			return new ArrayList<String>();
		}
		if (startYear == endYear) {// 同一年
			int startMonth = Integer.parseInt(startDate.substring(4, 6));
			int endMonth = Integer.parseInt(endDate.substring(4, 6));
			if (startMonth == endMonth) { // 同一个月
				List<String> dateStringList = DateUtils.getDateStringsOf(
						startYear, startMonth);
				int startDateInt = Integer.parseInt(startDate);
				int endDateInt = Integer.parseInt(endDate);
				for (String dateString : dateStringList) {
					int temp = Integer.parseInt(dateString);
					if ((startDateInt <= temp) && (temp <= endDateInt)) {
						dateList.add(dateString);
					}
				}
			} else { // 不同月
				// 先获取开始一个月的所有日期
				List<String> dateStringList = DateUtils.getDateStringsOf(
						startYear, startMonth);
				int startDateInt = Integer.parseInt(startDate);
				for (String dateString : dateStringList) {
					int temp = Integer.parseInt(dateString);
					if (temp >= startDateInt) {
						dateList.add(dateString);
					}
				}
				while (startMonth < endMonth) {
					startMonth += 1;
					dateList.addAll(DateUtils.getDateStringsOf(startYear,
							startMonth));
				}
				// 处理结束月的
				int endDateInt = Integer.parseInt(endDate);
				for (String dateString : dateStringList) {
					int temp = Integer.parseInt(dateString);
					if (temp <= endDateInt) {
						dateList.add(dateString);
					}
				}
			}
		} else { // 不同年

		}

		return dateList;
	}

	/**
	 * 验证日期格式是否正确
	 * 
	 * @param dateFormat
	 *            要验证的格式
	 * @return true表示格式正确，否则返回false。
	 */
	public static boolean validateDateFormat(String dateFormat) {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		sdf.setLenient(false);
		Date date;
		try {
			date = sdf.parse(dateFormat);
		} catch (ParseException e) {
			return false;
		}
		return date != null;
	}

	/**
	 * date1和date2格式为yyyyMMdd
	 */
	public static List<String> getBetweenDates(String date1, String date2) {
		List<String> list = new ArrayList<String>();
		DateFormat df = new SimpleDateFormat("yyyyMMdd");
		long begin = 0;
		long end = 0;
		try {
			begin = df.parse(date1).getTime();
			end = df.parse(date2).getTime();
		} catch (ParseException e) {
			e.printStackTrace();
		}
		String date = "";
		while (begin <= end) {
			date = df.format(new Date(begin));
			list.add(date);
			begin = begin + 3600 * 1000 * 24;// 每天24小时*3600秒*1000毫秒
		}
		return list;
	}
}
