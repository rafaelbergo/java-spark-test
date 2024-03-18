package com.LinearRegression;

import javax.xml.crypto.Data;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.linalg.DenseMatrix;
import org.apache.spark.ml.stat.Correlation;
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
        
        /* // Correlação 
        Dataset<Row> correlation1 = Correlation.corr(dadosFeatures, "features");
        //correlation1.show();
        Row linha1 = correlation1.first();
        DenseMatrix matrix1 = (DenseMatrix) linha1.get(0);
        System.out.println(matrix1.toString(30, 300));
        */

        // Regressão Linear
        org.apache.spark.ml.regression.LinearRegression lr = new org.apache.spark.ml.regression.LinearRegression();
        lr.setRegParam(0.3);
        org.apache.spark.ml.regression.LinearRegressionModel lrModel = lr.fit(dadosFeatures);
        // lrModel.transform(dadosFeatures).show();
        
        Dataset<Row>[] dadosTreinamentoTeste =  dadosFeatures.randomSplit(new double[] {0.8, 0.2});
        Dataset<Row> dadosTreinamento = dadosTreinamentoTeste[0];
        Dataset<Row> dadosTeste = dadosTreinamentoTeste[1];

        org.apache.spark.ml.regression.LinearRegressionModel lrModel2 = lr.fit(dadosTreinamento);
        lrModel2.transform(dadosTeste).show();

        System.out.println("R2: " + lrModel2.summary().r2());
        System.out.println("Root Mean Square Error: " + lrModel2.summary().rootMeanSquaredError());
        spark.close();
    }
}
