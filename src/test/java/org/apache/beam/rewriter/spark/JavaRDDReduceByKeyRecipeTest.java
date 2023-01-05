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
  void testRewriteFilter() {
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

}