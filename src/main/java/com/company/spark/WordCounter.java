package com.company.spark;

import java.util.Arrays;
import java.util.UUID;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class WordCounter {

  private static final String FILE_NAME = "samples/shakespeare.txt";

  public static void main(String[] args) {

    SparkConf sparkConf =
        new SparkConf()
            .setMaster("local")
            .setAppName("WordCount Sample")
            .set("spark.driver.bindAddress", "127.0.0.1")
            .set("spark.driver.host", "127.0.0.1");

    JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

    JavaRDD<String> inputFile = sparkContext.textFile(FILE_NAME);

    JavaRDD<String> wordsFromFile =
        inputFile
            .flatMap(content -> Arrays.stream(content.split(" ")).iterator())
            .map(word -> word.replaceAll("[^a-zA-Z0-9]", ""))
            .filter(word -> word.length() > 1);

    JavaPairRDD<String, Integer> countData =
        wordsFromFile.mapToPair(t -> new Tuple2<>(t, 1)).reduceByKey((x, y) -> x + y);

    countData.map(t2 -> t2._1 + "," + t2._2).saveAsTextFile("target/CountData/" + UUID.randomUUID());
  }
}
