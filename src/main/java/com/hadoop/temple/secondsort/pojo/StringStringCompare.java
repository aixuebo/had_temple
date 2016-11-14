package com.hadoop.temple.secondsort.pojo;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

/**
 * secondKey 按照倒序排序
 */
public class StringStringCompare implements WritableComparable<StringStringCompare>{

	protected String firstKey;  
	protected String secondKey;
      
    @Override  
    public void write(DataOutput out) throws IOException {  
        out.writeUTF(this.getFirstKey());  
        out.writeUTF(this.getSecondKey());  
    }  
  
    @Override  
    public void readFields(DataInput in) throws IOException {  
        this.setFirstKey(in.readUTF());
        this.setSecondKey(in.readUTF());
    }  
  
    //确保每一个partition中排序顺序是firstkey-secondKey顺序,至于在同一个reduce中,将key重新按照以下比较进行排序,即firstKey相同的就是同一个分组
    @Override  
    public int compareTo(StringStringCompare o) {
        int result = this.getFirstKey().compareTo(o.getFirstKey());
        return result!=0 ? result : -this.getSecondKey().compareTo(o.getSecondKey());  
    }  
  
    @Override  
    public String toString(){  
        return this.getFirstKey()+"\t"+this.getSecondKey();  
    }

	public String getFirstKey() {
		return firstKey;
	}

	public void setFirstKey(String firstKey) {
		this.firstKey = firstKey;
	}

	public String getSecondKey() {
		return secondKey;
	}

	public void setSecondKey(String secondKey) {
		this.secondKey = secondKey;
	}  
    
}
