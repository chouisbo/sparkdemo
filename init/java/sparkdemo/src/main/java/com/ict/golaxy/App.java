package com.ict.golaxy;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class App 
{
    public static void main( String[] args )
    {
		// Create a Java Spark Context
		SparkConf conf = new SparkConf().setMaster("spark://host29:7077").setAppName("My SparkApp Java");
		JavaSparkContext sc = new JavaSparkContext(conf);
    }
}

