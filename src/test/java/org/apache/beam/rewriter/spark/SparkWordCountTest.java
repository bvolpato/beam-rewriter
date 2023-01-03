package org.apache.beam.rewriter.spark;

import static org.openrewrite.java.Assertions.java;

import org.junit.jupiter.api.Test;
import org.openrewrite.java.JavaParser;
import org.openrewrite.test.RecipeSpec;
import org.openrewrite.test.RewriteTest;

class SparkWordCountTest implements RewriteTest {

  @Override
  public void defaults(RecipeSpec spec) {

    spec.recipe(new SparkMigrationCookbook())
        .parser(JavaParser.fromJavaVersion().classpath("beam", "spark", "scala"));

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
          
         class Convert {
             public void run(Pipeline pipeline) {
                 PCollection<String> rdd = pipeline.apply("ReadTextFile", TextIO.read().from("gs://beam-samples/shakespeare.txt"));

                 PCollection<String> filtered = rdd
                         .apply("Map", MapElements.via(word -> word.toUpperCase()))
                         .apply("Filter", Filter.by(word -> word.length() > 1));
                         
                 PCollection<KV<String, Integer>> pairRDD = filtered.apply("MapPair", MapElements.via(word -> KV.of(word, 1)));
             }
         }
        """));
  }
}