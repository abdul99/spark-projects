package com.abdul.bigdata.spark.partitions.airline;

import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

public class TestApp {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
      
	//	String text = "abcde||12345|ABCDE|||";
		String text = "abcde,12345,ABCDE,,";
		char separatorChar = ',';
		
		 String[] textArray = StringUtils.splitPreserveAllTokens(text, separatorChar);
		 List<String> list = Arrays.asList(textArray);
		 System.out.println(list);
		 System.out.println(list.size());
		 for (int i = 0; i < textArray.length; i++){
		//	 System.out.println(textArray[i]); 
			 
		 }
	}

}
