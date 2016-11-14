package com.hadoop.temple.util;

import java.math.BigDecimal;

public class Converter {

  public static String toString2(Object o) {
	    try {
	      if (o == null) {
	        return "";
	      } else if (o instanceof String) {
	        return (String) o;
	      } else if (o instanceof Integer) {
	        return String.valueOf((Integer) o);
	      } else if (o instanceof Long) {
	        return String.valueOf((Long) o);
	      } else if (o instanceof Short) {
	        return String.valueOf((Short) o);
	      } else if (o instanceof BigDecimal) {
	        return String.valueOf(((BigDecimal) o).doubleValue());
	      } else if (o instanceof Double) {
	        return String.valueOf(((Double) o).doubleValue());
	      } else if (o instanceof Float) {
	        return String.valueOf(((Float) o).floatValue());
	      }
	      return o.toString();
	    } catch (Exception ex) {
	      return "";
	    }
	  }

	public static String toString2(Object o, String defaultStr) {
		try {
			String result = "";
			if (o == null) {
				result = defaultStr;
			} else if (o instanceof String) {
				result = (String) o;
			} else if (o instanceof Integer) {
				result = String.valueOf((Integer) o);
			} else if (o instanceof Long) {
				result = String.valueOf((Long) o);
			} else if (o instanceof Short) {
				result = String.valueOf((Short) o);
			} else if (o instanceof BigDecimal) {
				result = String.valueOf(((BigDecimal) o).doubleValue());
			} else if (o instanceof Double) {
				result = String.valueOf(((Double) o).doubleValue());
			} else if (o instanceof Float) {
				result = String.valueOf(((Float) o).floatValue());
			} else {
				result = o.toString();
			}
			return StringUtil.trim(result, defaultStr);
		} catch (Exception ex) {
			return defaultStr;
		}
	}

	public static Integer converterToInteger(Object obj) {
		return converterToInteger(obj, 0);
	}

	public static Integer converterToInteger(Object o, int defaultValue) {
		try {
			if (o == null) {
				return defaultValue;
			} else if (o instanceof String) {
				return Integer.parseInt((String) o);
			} else if (o instanceof Integer) {
				return ((Integer) o).intValue();
			} else if (o instanceof Long) {
				return ((Long) o).intValue();
			} else if (o instanceof Short) {
				return ((Short) o).intValue();
			} else if (o instanceof BigDecimal) {
				return ((BigDecimal) o).intValue();
			} else if (o instanceof Double) {
				return ((Double) o).intValue();
			} else if (o instanceof Float) {
				return ((Float) o).intValue();
			} else if (o instanceof Boolean) {
				return Boolean.TRUE.equals(o) ? 1 : 0;
			}
			return Integer.parseInt(o.toString());
		} catch (Exception ex) {
			return defaultValue;
		}
	}

	public static Long converterToLong(Object obj) {
		return converterToLong(obj, 0l);
	}

	public static Long converterToLong(Object o, long defaultValue) {
		try {
			if (o == null) {
				return defaultValue;
			} else if (o instanceof String) {
				return Long.parseLong((String) o);
			} else if (o instanceof Integer) {
				return ((Integer) o).longValue();
			} else if (o instanceof Long) {
				return ((Long) o).longValue();
			} else if (o instanceof Short) {
				return ((Short) o).longValue();
			} else if (o instanceof BigDecimal) {
				return ((BigDecimal) o).longValue();
			} else if (o instanceof Double) {
				return ((Double) o).longValue();
			} else if (o instanceof Float) {
				return ((Float) o).longValue();
			} else if (o instanceof Boolean) {
				return Boolean.TRUE.equals(o) ? 1l : 0l;
			}
			return Long.parseLong(o.toString());
		} catch (Exception ex) {
			return defaultValue;
		}
	}
}
