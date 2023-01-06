package org.apache.beam.rewriter.spark;

import static org.openrewrite.java.Assertions.java;

import org.apache.beam.rewriter.common.CookbookEnum;
import org.apache.beam.rewriter.common.CookbookFactory;
import org.junit.jupiter.api.Test;
import org.openrewrite.test.RecipeSpec;
import org.openrewrite.test.RewriteTest;

class FunctionRecipeTest implements RewriteTest {

  @Override
  public void defaults(RecipeSpec spec) {

    spec.recipe(new FunctionRecipe())
        .parser(CookbookFactory.buildParser(CookbookEnum.SPARK));

  }

  @Test
  void testRewriteFunction() {

    rewriteRun(java("""
          import org.apache.spark.api.java.function.Function;
          
          class SumFunction implements Function<Integer, Integer> {
              @Override
              public Integer call(Integer v1) throws Exception {
                  return v1;
              }
          }
        """, """
          import org.apache.beam.sdk.transforms.SerializableFunction;
          
          class SumFunction implements SerializableFunction<Integer, Integer> {
              @Override
              public Integer apply(Integer v1) {
                  return v1;
              }
          }
        """));
    }

  @Test
  void testRewriteMapFunction() {

    rewriteRun(java("""
          import org.apache.spark.api.java.function.MapFunction;
          
          class SumFunction implements MapFunction<Integer, Integer> {
              @Override
              public Integer call(Integer v1) throws Exception {
                  return v1;
              }
          }
        """, """
          import org.apache.beam.sdk.transforms.SerializableFunction;
          
          class SumFunction implements SerializableFunction<Integer, Integer> {
              @Override
              public Integer apply(Integer v1) {
                  return v1;
              }
          }
        """));
  }

  @Test
  void testRewriteFunction2() {

    rewriteRun(java("""
          import org.apache.spark.api.java.function.Function2;
          
          class SumFunction implements Function2<Integer, Integer, Integer> {
              @Override
              public Integer call(Integer v1, Integer v2) throws Exception {
                  return v1 + v2;
              }
          }
        """, """
          import org.apache.beam.sdk.transforms.SerializableBiFunction;
          
          class SumFunction implements SerializableBiFunction<Integer, Integer, Integer> {
              @Override
              public Integer apply(Integer v1, Integer v2) {
                  return v1 + v2;
              }
          }
        """));
  }
}