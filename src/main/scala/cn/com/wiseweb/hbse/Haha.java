package cn.com.wiseweb.hbse;

import java.io.BufferedReader;
import java.io.FileReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;
import java.util.Stack;

public class Haha {
	/**
	 * 获取文件
	 * @param path
	 * @return
	 * @throws Exception
	 */
	public static String getFile(String path) throws Exception {
		StringBuffer sbf=new StringBuffer(); 
		String s="";
		
        BufferedReader br = new BufferedReader(new FileReader(path));//构造一个BufferedReader类来读取文件
        while((s = br.readLine())!=null){//使用readLine方法，一次读一行
        	//一行一行的追加
            sbf.append(s);
        }
        br.close();
        return sbf.toString();
	}
	
	public static String getUrlHash() {
		StringBuilder str=new StringBuilder();//定义变长字符串
		Random random=new Random();
		//随机生成数字，并添加到字符串
		int flage=(int)(Math.random()*10);
		if(flage>4){
			str.append("-");
		}
		for(int i=0;i<19;i++){
			int nextInt = random.nextInt(10);
//			//换算成16进制显示
//		    str.append(Integer.toHexString(nextInt));
			str.append(nextInt);
		}
		return str.toString();
	}
	
	public static String getDate(){
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
		return format.format(new Date());
	}
	
	
	public static String getStrToTime(String date){
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
		try {
			Date date2 = format.parse(date);
			return String.valueOf(date2.getTime());
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	/**
	 * 反转字符串
	 * @param s
	 * @return
	 */
	public static String reverseStr(String s) {
		char[] str = s.toCharArray();
		Stack<Character> stack = new Stack<Character>();
		for (int i = 0; i < str.length; i++)
			stack.push(str[i]);

		String reversed = "";
		for (int i = 0; i < str.length; i++)
			reversed += stack.pop();

		return reversed;
	}
	
	
	public static void main(String[] args) {
//		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
//		System.out.println(format.format(new Date()));
		
		String strToTime = getStrToTime("2016-08-26 10:25:39");
		System.out.println(strToTime);
//		String urlHash = getUrlHash();
//		System.out.println(urlHash);

	}
	
}
