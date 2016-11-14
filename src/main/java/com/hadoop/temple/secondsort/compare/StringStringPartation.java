package com.hadoop.temple.secondsort.compare;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import com.hadoop.temple.secondsort.pojo.StringStringCompare;


//按照first的key进行partition
public class StringStringPartation <K1, V1> extends Partitioner<K1, V1> {

	 @Override  
	    public int getPartition(K1 key, V1 value, int numPartitions) {  
		 StringStringCompare keyK= (StringStringCompare) key;  
	        Text first =new Text(keyK.getFirstKey());  
	        return (first.hashCode() & Integer.MAX_VALUE)%numPartitions;  
	    }  
	 
}
