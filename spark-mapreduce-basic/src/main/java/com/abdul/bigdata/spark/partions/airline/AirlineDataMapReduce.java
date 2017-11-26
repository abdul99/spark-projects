package com.abdul.bigdata.spark.partions.airline;

import java.io.File;
import java.util.Collection;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

import org.apache.spark.api.java.JavaRDD;

public class AirlineDataMapReduce {
	
	
	@SuppressWarnings("unchecked")
	public static void main(String[] args) throws Exception {
		System.out.println(System.getProperty("hadoop.home.dir"));
		
		String inputPath = args[0];
		String outputPath = args[1];
		float sampledAmt = Float.parseFloat(args[2]);
		
		/*Identify the original number of partitions*/
		String[] extensions = {"bz2"};		
		Collection<File> files = FileUtils.listFiles(new File(inputPath), extensions, false);
		int noOfPartitions = files.size();
		
		/*Delete output file. Do not do this in Production*/		
		FileUtils.deleteQuietly(new File(outputPath));

		/*Initialize Spark Context*/
		JavaSparkContext sc = new JavaSparkContext("local", "airlinedatasampler");
		
		
		/*Read in the data*/
		JavaRDD<String> rdd = sc.textFile(inputPath);

		/*Process the data*/
		rdd.filter(l->!(l.startsWith("Year")))  //Skip the header line
				       .sample(false, sampledAmt)
				       //.repartition(20)				       
				       .mapToPair(l->{
				    	   String[] parts = StringUtils.splitPreserveAllTokens(l, ",");
				           String yrMoDd = parts[0]+","+parts[1]+","+parts[2];
				           return new Tuple2<String,String>(yrMoDd,l);
				        })	//Map to key-value pair					        
				        .repartitionAndSortWithinPartitions(
				        		new AirlineCustomPartioner(noOfPartitions),//Partition the output as the original input
				        		new AirlineCustomComparator()) //Sort in ascending order by Year,Month and Day
				        .map(t->((Tuple2<String, String>) t)._2()) //Process just the value
				        .saveAsTextFile(outputPath); //Write to file		
		/*Close the context*/
		sc.close();
	}

}
