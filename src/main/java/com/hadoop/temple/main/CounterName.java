package com.hadoop.temple.main;

import com.hadoop.temple.job.SequenceFileToTextDriver;

/**
 * 各种计数器的名称 User: 马明 Date: 2015-07-15
 */
public class CounterName {

	public static class Parent {
		public static final String HANDLE_RECORD_TOTAL = "handle record total";
		public static final String HANDLE_RECORD_SUCCESS_TOTAL = "handle record success total";
		public static final String HANDLE_RECORD_FAIL_TOTAL = "handle record fail total";

		public static final String RECORD_TOTAL_MAPPER = "[map] record total";// 记录map阶段日子总记录数
		public static final String HANDLE_RECORD_SUCCESS_TOTAL_MAPPER = "[map] handle record success total";// 记录map阶段成功的
		public static final String HANDLE_RECORD_FAIL_TOTAL_MAPPER = "[map] handle record fail total";// 记录map阶段失败的

		public static final String INVALID_RECORD_MAPPER = "[map] invalid record total";
		public static final String VALID_RECORD_MAPPER = "[map] valid record total";

		public static final String RECORD_TOTAL_REDUCE = "[reduce] record total";
		public static final String HANDLE_RECORD_SUCCESS_TOTAL_REDUCE = "[reduce] handle record success total";
		public static final String HANDLE_RECORD_FAIL_TOTAL_REDUCE = "[reduce] handle record fail total";

	}

	public static class JoinGroup extends Parent {
		public static final String NAME = "Join Counters";

		public static final String ASP_VALID_RECORD_TOTAL_MAPPER = "[map] asp valid record total";
		public static final String HANDLE_ASP_RECORD_SUCCESS_TOTAL_MAPPER = "[map] handle asp record success total";
		public static final String HANDLE_ASP_RECORD_FAIL_TOTAL_MAPPER = "[map] handle asp record fail total";
		public static final String ASP_RECORD_TOTAL_MAPPER = "[map] asp record total";
		public static final String ASP_INVALID_PD_TOTAL_MAPPER = "[map] asp invalid pd(0) total";
		public static final String HANDLE_ASP_FILTER_IP_TOTAL = "handle asp filter ip total";
		public static final String ASP_INVALID_RECORD_MAPPER = "[map] asp invalid record total";
		public static final String ASP_MY_INVALID_RECORD_MAPPER = "[map] asp my invalid record total";

		public static final String CDP_VALID_RECORD_TOTAL_MAPPER = "[map] cdp valid record total";
		public static final String HANDLE_CDP_RECORD_SUCCESS_TOTAL_MAPPER = "[map] handle cdp record success total";
		public static final String HANDLE_CDP_RECORD_FAIL_TOTAL_MAPPER = "[map] handle cdp record fail total";
		public static final String CDP_RECORD_TOTAL_MAPPER = "[map] cdp record total";
		public static final String CDP_INVALID_RECORD_TOTAL_MAPPER = "[map] cdp invalid record total";

		public static final String CREATE_ASPLog_INSTANCE_ERROR_TOTAL_REDUCER = "[reducer] create ASPLog instance error total";
		public static final String CREATE_CDPLog_INSTANCE_ERROR_TOTAL_REDUCER = "[reducer] create CdpLog instance error total";
	}

	public static class TextToVectorSequenceFileDriverGroup extends Parent {
		public static final String NAME = "TextToVectorSequenceFileDriver Counters";
	}
	
	public static class SequenceFileToTextDriver extends Parent {
		public static final String NAME = "VectorSequenceFileToTextDriver Counters";
	}
	
}
