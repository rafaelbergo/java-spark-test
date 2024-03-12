package com.sparktest;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

public class App 
{
    public static void main( String[] args ) throws Exception {
        SparkSession spark = SparkSession.builder().appName("Spark Test").master("local[*]").getOrCreate();
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        Dataset<Row> emp = spark.read().option("header", true).option("inferSchema", true).csv("/home/rafael/Desktop/emp.csv");
        
        //emp.printSchema();
        //emp.show();
        //emp.select("emp_no", "first_name", "last_name").show();

        //Dataset<Row> emp2 = emp.filter(functions.col("emp_no").gt(10021)); // emp_no > 10021
        //Dataset<Row> emp2 = emp.filter(functions.col("emp_no").geq(10040)); // emp_no >= 10021
        //Dataset<Row> genderM = emp.filter(functions.col("gender").equalTo("M"));
        Dataset<Row> genderM = emp.where("gender = 'M'");
        
        System.out.println("Total de registros: " + genderM.count());
        

        spark.close();
    }
}
