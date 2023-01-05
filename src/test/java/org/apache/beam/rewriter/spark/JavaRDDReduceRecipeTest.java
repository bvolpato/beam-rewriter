package org.apache.beam.rewriter.spark;

import static org.openrewrite.java.Assertions.java;

import org.apache.beam.rewriter.common.CookbookEnum;
import org.apache.beam.rewriter.common.CookbookFactory;
import org.junit.jupiter.api.Test;
import org.openrewrite.test.RecipeSpec;
import org.openrewrite.test.RewriteTest;

class JavaRDDReduceRecipeTest implements RewriteTest {

  @Override
  public void defaults(RecipeSpec spec) {
    spec.recipe(new JavaRDDReduceRecipe())
        .parser(CookbookFactory.buildParser(CookbookEnum.SPARK));
  }

  @Test
  void testRewriteFilter() {
    rewriteRun(
        java(
            """
                  import org.apache.spark.api.java.JavaRDD;
                  
                  class Convert {
                    public void run(JavaRDD<Integer> rdd) {
                      JavaRDD<Integer> combined = rdd
                        .reduce((x, y) -> x + y);
                    }
                  }
                """,
            """
                  import org.apache.beam.sdk.transforms.Combine;
                  import org.apache.spark.api.java.JavaRDD;
                  
                  class Convert {
                    public void run(JavaRDD<Integer> rdd) {
                      JavaRDD<Integer> combined = rdd
                        .apply("CombineGlobally", Combine.globally((x, y) -> x + y));
                    }
                  }
                """
        )
    );
  }

}