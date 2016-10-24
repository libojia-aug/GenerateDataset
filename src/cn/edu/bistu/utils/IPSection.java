/**
 * 
 */
package cn.edu.bistu.utils;

import java.io.Serializable;
import java.util.Comparator;

/**
 * @author chenruoyu
 *
 */
public class IPSection implements Serializable,Comparable<Long>{
	
	/**
	 * 对网段进行排序，排序的依据是网段的起始地址，即start部分
	 * @author chenruoyu
	 *
	 */
	public static class IPSectionComparator implements Comparator<IPSection>{
		@Override
		public int compare(IPSection o1, IPSection o2) {
			return o1.compareTo(o2.start);
		}
	}
	
	
	private static final long serialVersionUID = 4923172892631812L;
	private long start = 0L;
	private long end = 0L;
	private int length = 0;		//IP段的长度
	private String value = null;
	
	public long getStart() {
		return start;
	}
	public void setStart(long start) {
		this.start = start;
	}
	
	public long getEnd() {
		return end;
	}
	public void setEnd(long end) {
		this.end = end;
	}
	public String getValue() {
		return value;
	}
	public void setValue(String value) {
		this.value = value;
	}
	@Override
	public boolean equals(Object obj) {
		if(obj==null){
			return false;
		}
		if(obj instanceof IPSection){
			IPSection sec = (IPSection)obj;
			if(value==null){
				return (start==sec.start) && (end==sec.end) && (sec.value==null);				
			}else{
				return (start==sec.start) && (end==sec.end) && (value.equals(sec.value));								
			}
		}else{
			return false;
		}
	}
	
	public int getLength() {
		return length;
	}
	public void setLength(int length) {
		this.length = length;
	}
	
	@Override
	public String toString() {
		return "["+start+"-"+end+"("+length+"):"+value+"]";
	}
	@Override
	public int compareTo(Long o) {
		return Long.compare(start, o);
	}
	/**
	 * 判断ip地址是否在当前IP段内
	 * @param ip
	 * @return
	 */
	public boolean inside(long ip){
		return (ip>=start) && (ip<=end);
	}
	/**
	 * 判断ip地址是否在当前IP段之后
	 * （即在数字上大于当前ip段的结束地址）
	 * @param ip
	 * @return
	 */
	public boolean after(long ip){
		if(ip>end){
			return true;
		}else{
			return false;
		}
	}
}
