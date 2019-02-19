package com.spark.demo;

import java.util.Arrays;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

public class SparkWordCount {
	
	public static void main (String arg[]) {
		
		SparkSession sparkSession = SparkSession.builder().master("local[2]").appName("wordcountspark").getOrCreate();
	        JavaSparkContext jsc = new JavaSparkContext(sparkSession.sparkContext());
	        JavaRDD<String> InputFileRdd = jsc.textFile("C:\\Amit\\MyWorkspace\\ApacheSparkSetup\\inputfile.txt"); 
	        

           JavaRDD<String> wordsFromFile = InputFileRdd.flatMap(content -> Arrays.asList(content.split(" ")).iterator());


           JavaPairRDD countData = wordsFromFile
        		                   .mapToPair(t -> new Tuple2(t, 1))
        		                   .reduceByKey((x, y) -> (int) x + (int) y)
        		                   .sortByKey();
           
           countData.foreach(data -> {
               System.out.println(((Tuple2) data)._1()+"-"+((Tuple2) data)._2());
           });

	}

}
