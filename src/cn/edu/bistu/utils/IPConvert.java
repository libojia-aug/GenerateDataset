/**
 * 
 */
package cn.edu.bistu.utils;

import java.io.Serializable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author chenruoyu
 *
 */
public class IPConvert implements Serializable{
	
	private static final long serialVersionUID = -6420091754407486844L;
	
	private static final String SINGLE = "([0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3})";
	
	private static final String RegEx_SINGLE_IP = "^"+SINGLE+"$";
	private static final Pattern PAT_SINGLE_IP = Pattern.compile(RegEx_SINGLE_IP);

	private static final String RegEx_IP_SEC_1 = "^"+SINGLE+"/([0-9]+)$";
	private static final Pattern PAT_IP_SEC_1 = Pattern.compile(RegEx_IP_SEC_1);

	private static final String RegEx_IP_SEC_2 = "^"+SINGLE+"-"+SINGLE+"$";
	private static final Pattern PAT_IP_SEC_2 = Pattern.compile(RegEx_IP_SEC_2);
	
	
	/**
	 * IP地址段转换过程中需要使用的掩码数组。
	 * 对于形如220.181.165.0/24的IP段，
	 * 已经包含了IP段的起始地址：220.181.165.0
	 * IP段的结束地址需要使用IP段中提供的段长度L（上例中为24）来进行计算，
	 * 方法是生成一个32位长的掩码，其中0-(L-1)位为0，
	 * L-31位为1，使用这个掩码与IP段的起始地址进行按位或操作，
	 * 即可得到IP段的结尾地址。
	 */
	private static final int[] masks = new int[]{
		0xFFFFFFFF,//L=0
		0x7FFFFFFF,//L=1
		0x3FFFFFFF,//L=2
		0x1FFFFFFF,//L=3
		0x0FFFFFFF,//L=4
		0x07FFFFFF,//L=5
		0x03FFFFFF,//L=6
		0x01FFFFFF,//L=7
		0x00FFFFFF,//L=8
		0x007FFFFF,//L=9
		0x003FFFFF,//L=10
		0x001FFFFF,//L=11
		0x000FFFFF,//L=12
		0x0007FFFF,//L=13
		0x0003FFFF,//L=14
		0x0001FFFF,//L=15
		0x0000FFFF,//L=16
		0x00007FFF,//L=17
		0x00003FFF,//L=18
		0x00001FFF,//L=19
		0x00000FFF,//L=20
		0x000007FF,//L=21
		0x000003FF,//L=22
		0x000001FF,//L=23
		0x000000FF,//L=24		
		0x0000007F,//L=25
		0x0000003F,//L=26
		0x0000001F,//L=27
		0x0000000F,//L=28
		0x00000007,//L=29
		0x00000003,//L=30
		0x00000001,//L=31
		0x00000000//L=32
	};
		
	/**
	 * 将字符串形式的IP地址转换为整数,
	 * 私有函数，不检查IP地址的格式。
	 * @param ipAddress
	 * @return
	 */
	private static long ipToLong(String ipAddress) {
		long result = 0;
		String[] ipAddressInArray = ipAddress.split("\\.");
		for (int i = 3; i >= 0; i--) {
			long ip = Long.parseLong(ipAddressInArray[3 - i]);
			/**
			 * IP地址的四个段，需要依次按位左移24，16，8，0位
			 * （对应于3，2，1，0个字节）.
			 * 按位左移后，依次与下一个段的整数进行按位或运算（OR）。
			 * 对于IP地址192.168.1.2：
			 * 192 << 24
			 * 168 << 16
			 * 1 << 8
			 * 2 << 0
			 */
			result |= ip << (i * 8);
		}
		return result;
	}

	/**
	 * 将字符串形式的IP地址转换为整数,
	 * 共有函数，需要先检查IP地址的格式。
	 * 如果格式不正确，将返回-1
	 * @param ipAddress
	 * @return
	 */
	public static long ip2Long(String ipAddress) {
		if (PAT_SINGLE_IP.matcher(ipAddress).find()){
			return ipToLong(ipAddress);
		}
		return -1;
	}	
	
	/**
	 * 将整数形式的IP地址转换为字符串形式
	 * @param i
	 * @return
	 */
	public static String longToIp(long i) {
		return ((i >> 24) & 0xFF) + "." + ((i >> 16) & 0xFF) + "." + ((i >> 8) & 0xFF) + "." + (i & 0xFF);
	}
	
	/**
	 * 根据字符串形式的IP段描述，解析出IP段对象.
	 * IP段有三种类型：
	 * 单独的IP地址
	 * IP地址/网段长度
	 * 起始IP地址-结束IP地址。
	 * 需要识别并解析上述三种地址段描述，并转换为IP段对象
	 * @param section
	 * @param value
	 * @return
	 */
	public static IPSection parseSection(String section, String value){
		if(section == null)
			return null;
		IPSection sec = new IPSection();
		sec.setValue(value);
		Matcher mat = PAT_SINGLE_IP.matcher(section);
		if(mat.find()){
			//单个IP地址
			long start = ipToLong(section);
			sec.setStart(start);
			sec.setEnd(start);
			sec.setLength(1);
		}else{
			mat = PAT_IP_SEC_1.matcher(section);
			if(mat.find()){
				//220.181.165.0/24形式的IP地址
				long start = ipToLong(mat.group(1));
				long end = start | masks[Integer.parseInt(mat.group(2))];
				sec.setStart(start);
				sec.setEnd(end);
				//System.out.println(end-start);
				sec.setLength(masks[Integer.parseInt(mat.group(2))]+1);
			}else{
				//220.181.165.0-220.181.165.255形式的IP地址
				mat = PAT_IP_SEC_2.matcher(section);
				if(mat.find()){
					long start = ipToLong(mat.group(1));
					long end = ipToLong(mat.group(2));
					if(start>end){
						/**
						 * 异常情况，
						 * 对于220.181.165.0-220.181.165.255形式的IP地址段，
						 * 有可能存在给定的起始地址大于结束地址的情况，
						 * 对于这种情况，应该认定为数据有误
						 */
						return null;
					}
					sec.setStart(start);
					sec.setEnd(end);
					sec.setLength((int)(end-start));
				}else{
					return null;
				}
			}
		}
		return sec;
	}	
}
