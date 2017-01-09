package com.ict.golaxy;

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._


object InitDemo {

    def main(args: Array[String]): Unit = {

        val conf = new SparkConf().setMaster("spark://host29:7077").setAppName("My SparkApp Scala")
        val sc = new SparkContext(conf)
    }
}


