package org.apache.beam.rewriter.spark;

import static org.openrewrite.java.Assertions.java;

import org.apache.beam.rewriter.common.CookbookEnum;
import org.apache.beam.rewriter.common.CookbookFactory;
import org.junit.jupiter.api.Test;
import org.openrewrite.test.RecipeSpec;
import org.openrewrite.test.RewriteTest;

class SparkWordCountTest implements RewriteTest {

  @Override
  public void defaults(RecipeSpec spec) {

    spec.recipe(new SparkMigrationCookbook())
        .parser(CookbookFactory.buildParser(CookbookEnum.SPARK));

  }

  @Test
  void testRewriteSparkWordCount() {
    rewriteRun(java("""
          import org.apache.spark.api.java.JavaPairRDD;
          import org.apache.spark.api.java.JavaRDD;
          import org.apache.spark.api.java.JavaSparkContext;
          import scala.Tuple2;
          
          class Convert {
            public void run(JavaSparkContext sparkContext) {
              JavaRDD<String> rdd = sparkContext.textFile("gs://beam-samples/shakespeare.txt");

              JavaRDD<String> filtered = rdd
                .map(word -> word.toUpperCase())
                .filter(word -> word.length() > 1);
                
              JavaPairRDD<String, Integer> pairRDD = filtered.mapToPair(word -> new Tuple2<>(word, 1));
            }
          }
        """, """
         import org.apache.beam.sdk.Pipeline;
         import org.apache.beam.sdk.io.TextIO;
         import org.apache.beam.sdk.transforms.Filter;
         import org.apache.beam.sdk.transforms.MapElements;
         import org.apache.beam.sdk.values.KV;
         import org.apache.beam.sdk.values.PCollection;
         import org.apache.beam.sdk.values.TypeDescriptors;

         class Convert {
             public void run(Pipeline pipeline) {
                 PCollection<String> rdd = pipeline.apply("ReadTextFile", TextIO.read().from("gs://beam-samples/shakespeare.txt"));

                 PCollection<String> filtered = rdd
                         .apply("Map", MapElements.into(TypeDescriptors.strings()).via(word -> word.toUpperCase()))
                         .apply("Filter", Filter.by(word -> word.length() > 1));
                         
                 PCollection<KV<String, Integer>> pairRDD = filtered.apply("MapToPair", MapElements.into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.integers())).via(word -> KV.of(word, 1)));
             }
         }
        """));
  }

  @Test
  void testHomeExample() {
    rewriteRun(java("""
        package com.company.spark;
                
        import java.util.Arrays;
        import java.util.UUID;
        import org.apache.spark.SparkConf;
        import org.apache.spark.api.java.JavaPairRDD;
        import org.apache.spark.api.java.JavaRDD;
        import org.apache.spark.api.java.JavaSparkContext;
        import scala.Tuple2;
                
        public class WordCounter {
                
          private static final String FILE_NAME = "gs://dataflow-samples/shakespeare/kinglear.txt";
                
          public static void main(String[] args) {
                
            SparkConf sparkConf = new SparkConf()
                .setMaster("local")
                .setAppName("WordCount Sample")
                .set("spark.driver.bindAddress", "127.0.0.1")
                .set("spark.driver.host", "127.0.0.1");
                
            JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
                
            JavaRDD<String> inputFile = sparkContext.textFile(FILE_NAME);
                
            JavaRDD<String> wordsFromFile = inputFile
                .flatMap(content -> Arrays.stream(content.split(" ")).iterator())
                .map(word -> word.replaceAll("[^a-zA-Z0-9]", ""))
                .filter(word -> word.length() > 1);
                
            JavaPairRDD<String, Integer> countData = wordsFromFile.mapToPair(t -> new Tuple2<>(t, 1))
                .reduceByKey((x, y) -> x + y);
            
            JavaRDD<String> result = countData.map(t2 -> t2._1 + "," + t2._2);
            result.foreach(line -> System.out.println(line));
        
            result.saveAsTextFile("target/CountData/" + UUID.randomUUID());
          }
        }
        """, """
        package com.company.spark;
                
        import java.util.Arrays;
        import java.util.UUID;
                
        import org.apache.beam.sdk.Pipeline;
        import org.apache.beam.sdk.io.TextIO;
        import org.apache.beam.sdk.options.PipelineOptions;
        import org.apache.beam.sdk.options.PipelineOptionsFactory;
        import org.apache.beam.sdk.transforms.Combine;
        import org.apache.beam.sdk.transforms.Filter;
        import org.apache.beam.sdk.transforms.FlatMapElements;
        import org.apache.beam.sdk.transforms.MapElements;
        import org.apache.beam.sdk.values.KV;
        import org.apache.beam.sdk.values.PCollection;
        import org.apache.beam.sdk.values.TypeDescriptors;
        
        public class WordCounter {
                
            private static final String FILE_NAME = "gs://dataflow-samples/shakespeare/kinglear.txt";
                
            public static void main(String[] args) {
                
                PipelineOptions pipelineOptions = PipelineOptionsFactory.create();
        
                Pipeline pipeline = Pipeline.create(pipelineOptions);
                
                PCollection<String> inputFile = pipeline.apply("ReadTextFile", TextIO.read().from(FILE_NAME));
                
                PCollection<String> wordsFromFile = inputFile
                        .apply("FlatMap", FlatMapElements.into(TypeDescriptors.strings()).via(content -> Arrays.asList(content.split(" "))))
                        .apply("Map", MapElements.into(TypeDescriptors.strings()).via(word -> word.replaceAll("[^a-zA-Z0-9]", "")))
                        .apply("Filter", Filter.by(word -> word.length() > 1));
                
                PCollection<KV<String, Integer>> countData = wordsFromFile.apply("MapToPair", MapElements.into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.integers())).via(t -> KV.of(t, 1)))
                        .apply("CombinePerKey", Combine.perKey((x, y) -> x + y));
                
                PCollection<String> result = countData.apply("Map", MapElements.into(TypeDescriptors.strings()).via(t2 -> t2.getKey() + "," + t2.getValue()));
                result.apply("ForEach", MapElements.into(TypeDescriptors.voids()).via(line -> {
                    System.out.println(line);
                    return null;
                }));
            
                result.apply("WriteTextFile", TextIO.write().to("target/CountData/" + UUID.randomUUID()));
                pipeline.run();
            }
        }
        """));
  }
}