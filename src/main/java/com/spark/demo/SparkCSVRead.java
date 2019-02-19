package com.spark.demo;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkCSVRead {

	public static void main(String[] args) {

		// Spark 2.0 use Spark Session to create context.
		SparkSession sparkSession = SparkSession.builder().master("local[2]").appName("csvspark").getOrCreate();

		// Read .csv file and create RDD (Dataset)
		Dataset<Row> inputCsv = sparkSession.read().option("header", "true").csv("src/main/resources/mycsv.csv");

		inputCsv.printSchema();
		/*
		 * root 
		 * |-- ID: string (nullable = true) 
		 * |-- NAME: string (nullable = true) 
		 * |-- ADDRESS: string (nullable = true)
		 */

		inputCsv.show(); // This will print all the records present in .csv file

		System.out.println(inputCsv.count()); // Total rows in the csv file.

	}

}
