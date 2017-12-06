package com.abdul.bigdata.spark.partitions.airline;

import static org.hamcrest.CoreMatchers.containsString;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.abdul.bigdata.spark.partions.employee.Employee_Key;
import com.abdul.bigdata.spark.partions.employee.Employee_Value;

import scala.Tuple2;

public class CustomFlightApp {

	public static void main(String[] args) {
		 
		SparkConf conf = new SparkConf().setAppName("pattern").setMaster("local");

		JavaSparkContext jsc = new JavaSparkContext(conf);
		
		String inputPath = args[0];
		String outputPath = args[1];
		FileUtils.deleteQuietly(new File(outputPath));
		
		JavaRDD<String> rdd = jsc.textFile(inputPath);
		
		JavaRDD<String> rddAfterHeader =  rdd.filter(l->!(l.contains("YEAR")));
		
       JavaPairRDD<CustomFlightKey, CustomFlightValues> rddKeyValues = rddAfterHeader.mapToPair((value -> {
			
			String data = value.toString();
			String[] field =  StringUtils.splitPreserveAllTokens(data, ",");
			
			/**F17 = DEST_AIRPORT_ID  
			 * F6 = UNIQUE_CARRIER
			 * F20 =  ARR_DELAY
			 */
			 

			return new Tuple2<CustomFlightKey, CustomFlightValues>(
					new CustomFlightKey(field[6], field[17],
							field[21].length() > 0 ? Double.parseDouble(field[21]) : 0.0),
					new CustomFlightValues(field[5], field[10], field[12],field[18]));
				 
		}));
       
       
       JavaPairRDD<CustomFlightKey, CustomFlightValues> rddAfterPartition =  rddKeyValues.repartitionAndSortWithinPartitions(new CustomFlightPartitioner(13));
       
       JavaPairRDD<String, String> rddPair = rddAfterPartition.mapToPair((keyValues -> {
    	    String key = keyValues._1.toString();
    	    String value = keyValues._2.toString();
    	    return new Tuple2<String, String>(key,value);
    	 
            }));
       
       rddPair.saveAsTextFile(outputPath);
    //   rddAfrerPartition.collect();
    //   rddPair.saveAsTextFile(outputPath);
       
       /*for (Tuple2<CustomFlightKey, CustomFlightValues> data : rddAfrerPartition.collect()) {

    	   System.out.println(data._1.toString() + data._2.toString()); 

		}*/

	}

}
