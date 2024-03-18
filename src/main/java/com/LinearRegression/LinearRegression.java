package com.LinearRegression;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class LinearRegression {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().appName("DecisionTree").master("local[*]").getOrCreate();
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        Dataset<Row> dados = spark.read().option("header", true).option("inferSchema", true).csv("/home/rafael/Desktop/kc_house_data.csv");

        //dados.show();
        //dados.printSchema();
        //dados.summary().show();
 

        VectorAssembler assembler = new VectorAssembler()
            .setInputCols(new String[] {"bedrooms", "bathrooms", "sqft_living", "sqft_lot", "sqft_above", "sqft_basement"})
            .setOutputCol("features")
            .setHandleInvalid("skip"); // pula a linha que tiver algum valor nulo
        
        Dataset<Row> dadosFeatures = assembler.transform(dados).select("price", "features").withColumnRenamed("price", "label");
        //dadosFeatures.show();

        //LinearRegression lr = new LinearRegression();
        //LinearRegressionModel lrModel = lr.fit(dadosFeatures);

        // Create a LinearRegression object
        org.apache.spark.ml.regression.LinearRegression lr = new org.apache.spark.ml.regression.LinearRegression();

        // Fit the model to the data
        org.apache.spark.ml.regression.LinearRegressionModel lrModel = lr.fit(dadosFeatures);
        
        lrModel.transform(dadosFeatures).show();

        spark.close();
    }
}
