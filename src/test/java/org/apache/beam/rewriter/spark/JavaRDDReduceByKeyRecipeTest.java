package org.apache.beam.rewriter.spark;

import static org.openrewrite.java.Assertions.java;

import org.apache.beam.rewriter.common.CookbookEnum;
import org.apache.beam.rewriter.common.CookbookFactory;
import org.junit.jupiter.api.Test;
import org.openrewrite.test.RecipeSpec;
import org.openrewrite.test.RewriteTest;

class JavaRDDReduceByKeyRecipeTest implements RewriteTest {

  @Override
  public void defaults(RecipeSpec spec) {
    spec.recipe(new JavaRDDReduceByKeyRecipe())
        .parser(CookbookFactory.buildParser(CookbookEnum.SPARK));
  }

  @Test
  void testRewriteInline() {
    rewriteRun(
        java(
            """
                  import org.apache.spark.api.java.JavaPairRDD;
                  
                  class Convert {
                    public void run(JavaPairRDD<String, Integer> rdd) {
                      JavaPairRDD<String, Integer> combined = rdd
                        .reduceByKey((x, y) -> x + y);
                    }
                  }
                """,
            """
                  import org.apache.beam.sdk.transforms.Combine;
                  import org.apache.spark.api.java.JavaPairRDD;
                  
                  class Convert {
                    public void run(JavaPairRDD<String, Integer> rdd) {
                      JavaPairRDD<String, Integer> combined = rdd
                        .apply("CombinePerKey", Combine.perKey((x, y) -> x + y));
                    }
                  }
                """
        )
    );
  }

  @Test
  void testRewriteFunction() {
    rewriteRun(
        java(
            """
                  import org.apache.spark.api.java.JavaPairRDD;
                  import org.apache.spark.api.java.function.Function2;
                  
                  class Convert {
                    public void run(JavaPairRDD<String, Integer> rdd) {
                      JavaPairRDD<String, Integer> combined = rdd
                        .reduceByKey(new SumFunction());
                    }

                    static class SumFunction implements Function2<Integer, Integer, Integer> {
                      @Override
                      public Integer call(Integer v1, Integer v2) throws Exception {
                        return v1 + v2;
                      }
                    }
                  }
                """,
            """
                  import org.apache.beam.sdk.transforms.Combine;
                  import org.apache.spark.api.java.JavaPairRDD;
                  import org.apache.spark.api.java.function.Function2;
                  
                  class Convert {
                    public void run(JavaPairRDD<String, Integer> rdd) {
                      JavaPairRDD<String, Integer> combined = rdd
                        .apply("CombinePerKey", Combine.perKey(new SumFunction()));
                    }

                    static class SumFunction implements Function2<Integer, Integer, Integer> {
                      @Override
                      public Integer call(Integer v1, Integer v2) throws Exception {
                        return v1 + v2;
                      }
                    }
                  }
                """
        )
    );
}
  }