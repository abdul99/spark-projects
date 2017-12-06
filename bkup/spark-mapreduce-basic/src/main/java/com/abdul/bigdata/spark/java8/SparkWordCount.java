package com.abdul.bigdata.spark.java8;

 
import java.io.File;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;


/**
 * This is a simple example of Spark of a counter, well explained and verbose about Spark and it components.
 * The word count example read a file from the an input file and count all the words in the file. The present version is based in java 8 lambda expressions
 *
 * Readme some extra documentation:
 *   1- http://stackoverflow.com/questions/19620642/failed-to-locate-the-winutils-binary-in-the-hadoop-binary-path
 */

public class SparkWordCount {

    public static void main(String[] args) throws Exception {

		System.out.println(System.getProperty("hadoop.home.dir"));

        String inputPath = args[0];
		String outputPath = args[1];

		FileUtils.deleteQuietly(new File(outputPath));

		 List<Tuple2<String, Integer>> finalCounts =  new WordsCounterSpark().count(inputPath);
        
		 for(Tuple2<String, Integer> count: finalCounts)
             System.out.println(count._1() + " " + count._2());
	}


}
