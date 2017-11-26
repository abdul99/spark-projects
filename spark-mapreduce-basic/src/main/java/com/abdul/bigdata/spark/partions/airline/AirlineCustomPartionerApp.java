package com.abdul.bigdata.spark.partions.airline;

import java.io.File;
import java.util.Collection;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.JavaSparkContext;
 

import scala.Tuple2;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

public class AirlineCustomPartionerApp {
	
	
	@SuppressWarnings("unchecked")
	public static void main(String[] args) throws Exception {
		System.out.println(System.getProperty("hadoop.home.dir"));
		
		String inputPath = args[0];
		String outputPath = args[1];
		float sampledAmt = Float.parseFloat(args[2]);
		
		/*Identify the original number of partitions*/
		String[] extensions = {"csv","xls","xlsx"};		
		Collection<File> files = FileUtils.listFiles(new File(inputPath), extensions, true);
		//Collection<File> files = FileUtils.listFiles(new File(inputPath));
		int noOfPartitions = files.size();
		//int noOfPartitions = 3;
		System.out.println("no of partions: " + noOfPartitions );
		/*Delete output file. Do not do this in Production*/		
		FileUtils.deleteQuietly(new File(outputPath));

		/*Initialize Spark Context*/
		JavaSparkContext sc = new JavaSparkContext("local", "airlinedatasampler");
		
		
		/*Read in the data*/
		JavaRDD<String> rdd = sc.textFile(inputPath);
		
/*		JavaRDD<String> rddAfterHeader =  rdd.filter(l->!(l.startsWith("Year")))
 				.sample(false, sampledAmt);*/
		
	/*	JavaRDD<String> rddAfterHeader =  rdd.filter(l->!(l.startsWith("Year")))
				.sample(false, sampledAmt);*/
		
		JavaRDD<String> rddAfterHeader =  rdd.filter(l->!(l.startsWith("Year")));
				//.sample(false, sampledAmt);
		
 
		
		JavaPairRDD<String, String>	pair = rddAfterHeader .mapToPair(record -> {
		 										String[] parts = StringUtils.splitPreserveAllTokens(record, ",");
		 									//	System.out.println(record.toString());
		 										String yrMoDd = parts[0]+","+parts[1]+","+parts[2];
		 										return new Tuple2<String,String>(yrMoDd,record);
		 			                        	});
		
		
		JavaPairRDD<String, String>	partitionAfterPair  = pair.repartitionAndSortWithinPartitions(new AirlineCustomPartioner(noOfPartitions),new AirlineCustomComparator());
		 
		partitionAfterPair.map(t -> t._2).saveAsTextFile(outputPath);
 
	 
		sc.close();
	}

}
