package com.hadoop.temple.secondsort.compare;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import com.hadoop.temple.secondsort.pojo.StringStringCompare;


//在同一个reduce中,将key重新按照以下比较进行排序,即firstKey相同的就是同一个分组
public class StringStringGroup extends WritableComparator {

	 protected StringStringGroup(){  
	    super(StringStringCompare.class,true);  
	 }  
	 
    @SuppressWarnings("rawtypes")  
    @Override  
    public int compare(WritableComparable a,WritableComparable b){  
    	StringStringCompare ak = (StringStringCompare) a;  
    	StringStringCompare bk = (StringStringCompare) b;  
        return ak.getFirstKey().compareTo(bk.getFirstKey());  
    }  
	 
}
