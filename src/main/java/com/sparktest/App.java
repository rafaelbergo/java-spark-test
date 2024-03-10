package com.sparktest;

//import java.util.logging.Logger;


import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


public class App 
{
    public static void main( String[] args ) throws Exception {
        SparkSession spark = SparkSession.builder().appName("Spark Test").master("local[*]").getOrCreate();
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        
        Dataset<Row> emp = spark.read().option("header", true).csv("path");
        emp.show();
        spark.close();

    }
}
