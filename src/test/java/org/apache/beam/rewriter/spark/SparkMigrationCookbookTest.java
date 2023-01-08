package org.apache.beam.rewriter.spark;

import static org.openrewrite.java.Assertions.java;

import org.apache.beam.rewriter.common.CookbookEnum;
import org.apache.beam.rewriter.common.CookbookFactory;
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

  @Test
  void testRewriteFilterReduceLambda() {

    rewriteRun(java("""
          import org.apache.spark.api.java.JavaRDD;

          class Convert {
              public void run(JavaRDD<Integer> rdd) {
                JavaRDD<Integer> positive = rdd.filter(x -> x > 0);
                JavaRDD<Integer> sum = positive.reduce((x, y) -> x + y);
              }
          }
        """, """
         import org.apache.beam.sdk.transforms.Combine;
         import org.apache.beam.sdk.transforms.Filter;
         import org.apache.beam.sdk.values.PCollection;

         class Convert {
             public void run(PCollection<Integer> rdd) {
                 PCollection<Integer> positive = rdd.apply("Filter", Filter.by(x -> x > 0));
                 PCollection<Integer> sum = positive.apply("CombineGlobally", Combine.globally((x, y) -> x + y));
             }
         }
        """));
  }

  @Test
  void testRewriteReduceFilterLambda() {
    rewriteRun(java("""
          import org.apache.spark.api.java.JavaRDD;
          import scala.Tuple2;

          class Convert {
              public void run(JavaRDD<String> rdd) {
                rdd.mapToPair(word -> new Tuple2<>(word, 1)).reduceByKey((x, y) -> x + y).filter(t -> t._2 > 1);
              }
          }
        """, """
         import org.apache.beam.sdk.transforms.Combine;
         import org.apache.beam.sdk.transforms.Filter;
         import org.apache.beam.sdk.transforms.MapElements;
         import org.apache.beam.sdk.values.KV;
         import org.apache.beam.sdk.values.PCollection;
         import org.apache.beam.sdk.values.TypeDescriptors;
         
         class Convert {
             public void run(PCollection<String> rdd) {
                 rdd.apply("MapToPair", MapElements.into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.integers())).via(word -> KV.of(word, 1))).apply("CombinePerKey", Combine.perKey((x, y) -> x + y)).apply("Filter", Filter.by(t -> t.getValue() > 1));
             }
         }
        """));
  }
}