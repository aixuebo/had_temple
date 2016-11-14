package com.hadoop.temple.secondsort.pojo;

import org.apache.hadoop.io.WritableComparable;

/**
 * secondKey 按照正序排序
 */
public class StringStringAscCompare extends StringStringCompare implements WritableComparable<StringStringCompare>{
	    //确保每一个partition中排序顺序是firstkey-secondKey顺序,至于在同一个reduce中,将key重新按照以下比较进行排序,即firstKey相同的就是同一个分组
	    @Override  
	    public int compareTo(StringStringCompare o) {
	        int result = this.getFirstKey().compareTo(o.getFirstKey());
	        return result!=0 ? result : this.getSecondKey().compareTo(o.getSecondKey());  
	    }  
}
