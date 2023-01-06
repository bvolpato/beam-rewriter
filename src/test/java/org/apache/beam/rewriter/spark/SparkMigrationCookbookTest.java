package org.apache.beam.rewriter.spark;

import static org.openrewrite.java.Assertions.java;

import org.apache.beam.rewriter.common.CookbookEnum;
import org.apache.beam.rewriter.common.CookbookFactory;
import org.apache.spark.api.java.JavaRDD;
import org.junit.jupiter.api.Test;
import org.openrewrite.test.RecipeSpec;
import org.openrewrite.test.RewriteTest;

class SparkMigrationCookbookTest implements RewriteTest {

  @Override
  public void defaults(RecipeSpec spec) {

    spec.recipe(new SparkMigrationCookbook())
        .parser(CookbookFactory.buildParser(CookbookEnum.SPARK));

  }

  @Test
  void testRewriteReduce() {

    rewriteRun(java("""
          import org.apache.spark.api.java.JavaPairRDD;
          import org.apache.spark.api.java.JavaRDD;
          import org.apache.spark.api.java.JavaSparkContext;
          import scala.Tuple2;
          
          class Convert {
            public void run(JavaRDD<Integer> rdd) {
              rdd.reduce((x, y) -> x + y);
            }
          }
        """, """
         import org.apache.beam.sdk.transforms.Combine;
         import org.apache.beam.sdk.values.PCollection;

         class Convert {
             public void run(PCollection<Integer> rdd) {
                 rdd.apply("CombineGlobally", Combine.globally((x, y) -> x + y));
             }
         }
        """));
  }

  @Test
  void testRewriteReduceFunctionRef() {

    rewriteRun(java("""
          import org.apache.spark.api.java.JavaPairRDD;
          import org.apache.spark.api.java.JavaRDD;
          import org.apache.spark.api.java.JavaSparkContext;
          import scala.Tuple2;
          
          class Convert {
            public void run(JavaRDD<Integer> rdd) {
              rdd.reduce(Integer::sum);
            }
          }
        """, """
         import org.apache.beam.sdk.transforms.Combine;
         import org.apache.beam.sdk.values.PCollection;

         class Convert {
             public void run(PCollection<Integer> rdd) {
                 rdd.apply("CombineGlobally", Combine.globally(Integer::sum));
             }
         }
        """));
  }

  @Test
  void testRewriteReduceFunctionStatic() {

    rewriteRun(java("""
          import org.apache.spark.api.java.JavaPairRDD;
          import org.apache.spark.api.java.JavaRDD;
          import org.apache.spark.api.java.JavaSparkContext;
          import org.apache.spark.api.java.function.Function2;
          import scala.Tuple2;
          
          class Convert {
              public void run(JavaRDD<Integer> rdd) {
                rdd.reduce(new SumFunction());
              }
            
              static class SumFunction implements Function2<Integer, Integer, Integer> {
                  @Override
                  public Integer call(Integer v1, Integer v2) throws Exception {
                      return v1 + v2;
                  }
              }
          }
        """, """
         import org.apache.beam.sdk.transforms.Combine;
         import org.apache.beam.sdk.transforms.SerializableBiFunction;
         import org.apache.beam.sdk.values.PCollection;

         class Convert {
             public void run(PCollection<Integer> rdd) {
                 rdd.apply("CombineGlobally", Combine.globally(new SumFunction()));
             }
             
             static class SumFunction implements SerializableBiFunction<Integer, Integer, Integer> {
                 @Override
                 public Integer apply(Integer v1, Integer v2) {
                     return v1 + v2;
                 }
             }
         }
        """));
  }
}