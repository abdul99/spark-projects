package com.abdul.bigdata.spark.joins;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class MovieJoins {

	public static void main(String[] args) throws IOException {
		
		     SparkConf conf = new SparkConf().setAppName("spark-java8-joins").setMaster("local");
	         JavaSparkContext sc = new JavaSparkContext(conf);
	         
	         if (args.length == 0) {
	             args = new String[10];
	             args[0] = "src/main/resources/data/joins/data_1.txt";
	             args[1] =  "src/main/resources/data/joins/data_2.txt";
	             args[2] = "src/main/resources/data/joins/data_1_2_join.txt";
	             
	             args[3] = "src/main/resources/data/joins/data_1__join.txt";
	             args[4] = "src/main/resources/data/joins/data_2_join.txt";
	             
	             args[5] = "src/main/resources/data/joins/data_2_1_join.txt";
	            
	             FileUtils.deleteDirectory(new File(args[2]));
	             FileUtils.deleteDirectory(new File(args[3]));
	             FileUtils.deleteDirectory(new File(args[4]));
	             
	             FileUtils.deleteDirectory(new File(args[5]));
	         
	            
	         }
	         
	         JavaRDD<String> rdd1 =    sc.textFile(args[0]);
	         JavaRDD<String> rdd2 =    sc.textFile(args[1]);
	         JavaPairRDD<String, Double> pairRdd1 =  rdd1.mapToPair((record -> {
	        	 String[] data = record.split(",", -1);
	        	 return new Tuple2<String, Double>(data[0], Double.parseDouble(data[1]));
	     	 
	             }));
	         
	         JavaPairRDD<String, String> pairRdd2 =  rdd2.mapToPair((record -> {
	        	 String[] data = record.split(",", -1);
	        	 return new Tuple2<String, String>(data[1], data[2]);
	     	 
	             }));
	         
	         pairRdd1.saveAsTextFile(args[3]);
	         pairRdd2.saveAsTextFile(args[4]);
	         
	         JavaPairRDD<String, Tuple2<Double, String>> innerRdd=pairRdd1.join(pairRdd2);
	         innerRdd.saveAsTextFile(args[5]);
	         
	         JavaPairRDD<String, Tuple2<String, Double>> innerRddt=pairRdd2.join(pairRdd1);
	         innerRddt.saveAsTextFile(args[2]);
	         
	         
	         
	         
	         
	         
	}
}
