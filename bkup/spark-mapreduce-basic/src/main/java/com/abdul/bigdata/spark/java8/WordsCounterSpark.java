/*
 *
 *  * Copyright (c) 2015 Ivan Hristov <hristov[DOT]iv[AT]gmail[DOT]com>
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  * 	http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package com.abdul.bigdata.spark.java8;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;


public class WordsCounterSpark implements Serializable {

    public static final String REGEX = "\\s+";

    public List<Tuple2<String, Integer>> count(String source) {

        SparkConf conf = new SparkConf().setAppName("ingini-spark-java8").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> input = sc.textFile(source);

        JavaPairRDD<String, Integer> counts = input.flatMap(line -> Arrays.asList(line.split(REGEX)).iterator())
                .mapToPair(word -> new Tuple2(word, 1))
                .reduceByKey((x, y) -> (Integer) x + (Integer) y)
                .sortByKey();

         return counts.collect();
    }
    
    
    /* public List<Tuple2<String, Integer>> count(String source) {

    SparkConf conf = new SparkConf().setAppName("ingini-spark-java8").setMaster("local");
    JavaSparkContext sc = new JavaSparkContext(conf);

    JavaRDD<String> input = sc.textFile(source);

    JavaPairRDD<String, Integer> counts = input.flatMap(line -> Arrays.asList(line.split(REGEX)))
            .mapToPair(word -> new Tuple2(word, 1))
            .reduceByKey((x, y) -> (Integer) x + (Integer) y)
            .sortByKey();

     return counts.collect();
}*/
    
    
   // JavaSparkContext sc = new JavaSparkContext();
    
    // Parallelized with 2 partitions
   /* JavaRDD<String> rddX = sc.parallelize(
            Arrays.asList("spark rdd example", "sample example"),
            2);
    
    // map operation will return List of Array in following case
    JavaRDD<String[]> rddY = rddX.map(e -> e.split(" "));
    List<String[]> listUsingMap = rddY.collect();
    
    // flatMap operation will return list of String in following case
    JavaRDD<String> rddY2 = rddX.flatMap(e -> Arrays.asList(e.split(" ")).iterator());
    List<String> listUsingFlatMap = rddY2.collect();
    
    
    
    JavaRDD<String> textFile = sc.textFile("hdfs://...");
    JavaPairRDD<String, Integer> counts = textFile
        .flatMap(s -> Arrays.asList(s.split(" ")).iterator())
        .mapToPair(word -> new Tuple2<>(word, 1))
        .reduceByKey((a, b) -> a + b);*/
}



