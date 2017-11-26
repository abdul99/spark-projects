package com.abdul.bigdata.spark.partions.employee;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

public class CustomEmployerApp {

	public static void main(String[] args) {

		SparkConf conf = new SparkConf().setAppName("pattern").setMaster("local");

		JavaSparkContext jsc = new JavaSparkContext(conf);

		JavaRDD<String> rdd = jsc.textFile(args[0]);

		/*JavaPairRDD<Employee_Key, Employee_Value> pair = rdd
				.mapToPair(new PairFunction<String, Employee_Key, Employee_Value>() {

					@Override
					public Tuple2<Employee_Key, Employee_Value> call(String value) throws Exception {
						// TODO Auto-generated method stub

						String data = value.toString();
						String[] field = data.split(",", -1);

						return new Tuple2<Employee_Key, Employee_Value>(
								new Employee_Key(field[0], field[3],
										field[7].length() > 0 ? Double.parseDouble(field[7]) : 0.0),
								new Employee_Value(field[2], field[1], field[4]));

					}
				});
		*/
		
		JavaPairRDD<Employee_Key, Employee_Value> pair = rdd.mapToPair((value -> {
			
			String data = value.toString();
			String[] field = data.split(",", -1);

			return new Tuple2<Employee_Key, Employee_Value>(
					new Employee_Key(field[0], field[3],
							field[7].length() > 0 ? Double.parseDouble(field[7]) : 0.0),
					new Employee_Value(field[2], field[1], field[4]));
		}));

		JavaPairRDD<Employee_Key, Employee_Value> output = pair
				.repartitionAndSortWithinPartitions(new CustomEmployeePartitioner(50));

		for (Tuple2<Employee_Key, Employee_Value> data : output.collect()) {

			System.out.println(data._1.getDepartment() + " " + data._1.getSalary() + " " + data._1.getName()
					+ data._2.getDesignation() + " " + data._2.getJobtype() + " " + data._2.getLastname());

		}

	}

}