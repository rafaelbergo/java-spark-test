package com.DecisionTree;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.classification.DecisionTreeClassificationModel;
import org.apache.spark.ml.classification.DecisionTreeClassifier;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;

public class DecisionTree {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().appName("DecisionTree").master("local[*]").getOrCreate();
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        
        Dataset<Row> dados = spark.read().option("header", true).option("inferSchema", true).csv("/home/rafael/Desktop/diabetes.csv");

        dados.show();
        dados.printSchema();
        dados.summary().show(); // resume os dados

        VectorAssembler assembler = new VectorAssembler().setInputCols(new String[] {"Glucose", "BloodPressure", "SkinThickness", "Insulin", "BMI", "Age"}).setOutputCol("features");

        Dataset<Row> dadosFeatures = assembler.transform(dados).select("Outcome", "features").withColumnRenamed("Outcome", "label");
        dadosFeatures.show();

        DecisionTreeClassifier dtc = new DecisionTreeClassifier();
        //dtc.setMaxDepth(20); // define a profundidade máxima da árvore para melhorar a precisão
        DecisionTreeClassificationModel DTmodel = dtc.fit(dadosFeatures);

        DTmodel.transform(dadosFeatures).show();

        System.out.println(DTmodel.toDebugString()); // imprime a árvore de decisão

        Dataset<Row> dadosTreinamentoTotal[] = dadosFeatures.randomSplit(new double[] {0.8, 0.2});
        Dataset<Row> dadosTreinamento = dadosTreinamentoTotal[0];
        Dataset<Row> dadosTeste = dadosTreinamentoTotal[1];

        DecisionTreeClassificationModel DTmodel2 = dtc.fit(dadosTreinamento);
        Dataset<Row> previsoes = DTmodel2.transform(dadosTeste);

        previsoes.show();
        
        MulticlassClassificationEvaluator avaliador = new MulticlassClassificationEvaluator().setMetricName("accuracy"); // avalia a precisão do modelo
        System.out.println("A precisão do modelo é: " + avaliador.evaluate(previsoes)); 

        spark.close();
    }
}
