package com.spark.demo;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

class SparkCSVReadGroupBy {

		public static void main(String[] args) {

			// Spark 2.0 use Spark Session to create context.
			SparkSession sparkSession = SparkSession.builder().master("local[4]").appName("myspark").getOrCreate();			

			/**
			 * Creating Custom schema for the .csv file columns
			 **/
			StructType schema = DataTypes.createStructType(new StructField[] {
		            DataTypes.createStructField("EMPLOYEE_ID",  DataTypes.StringType, true),
		            DataTypes.createStructField("EMPLOYEE_NAME", DataTypes.StringType, true),
		            DataTypes.createStructField("CITY", DataTypes.StringType, true),
		            DataTypes.createStructField("COUNTRY", DataTypes.StringType, true),
		            DataTypes.createStructField("SALARY", DataTypes.IntegerType, true),
		            DataTypes.createStructField("YEAR", DataTypes.StringType, true),
		            DataTypes.createStructField("RATING", DataTypes.StringType, true)
		    });

					
			/** 1. Creating RDD(Dataset) from .csv file  **/
				Dataset<Row> csvDataSet  = sparkSession.read()
						                               .option("header","true").schema(schema)
						                               .csv("src/main/resources/Emp_1.csv");
				
				System.out.println("Printing .csv file Schema");
				csvDataSet.printSchema(); //This will print the schema 
				
				System.out.println("Printing .csv file all Data");
				csvDataSet.show(); // This will print the data present in .csv file 

			
			/** 2. Count the number of employees present in the .csv file **/
				Dataset employeeCountDataFrame = csvDataSet.groupBy("EMPLOYEE_ID").count();		
				
				System.out.println("Printing number of Employees");
				employeeCountDataFrame.show();	   // Printing employee count	
				
				Dataset employeeCountDataFrameRemovedNULL = employeeCountDataFrame.filter("employee_id != 'null'");// filtering out null rows		
				employeeCountDataFrameRemovedNULL.show(); // Removing null row


			/** 3. Group by and aggregated by salary **/
				Dataset groupByEmployeeIdSumSalary = csvDataSet.groupBy("EMPLOYEE_ID").sum(// @formatter:off
				                                                                           // @formatter:on
						                                                                 );
				System.out.println("Printing Employees and their Salaries");
				groupByEmployeeIdSumSalary.show();
			

		} // main method end

		/****/

	}// class end