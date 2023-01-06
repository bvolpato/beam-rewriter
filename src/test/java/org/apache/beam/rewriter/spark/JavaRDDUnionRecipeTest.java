package org.apache.beam.rewriter.spark;

import static org.openrewrite.java.Assertions.java;

import org.apache.beam.rewriter.common.CookbookEnum;
import org.apache.beam.rewriter.common.CookbookFactory;
import org.apache.beam.rewriter.common.DebuggingRecipe;
import org.junit.jupiter.api.Test;
import org.openrewrite.test.RecipeSpec;
import org.openrewrite.test.RewriteTest;

class JavaRDDUnionRecipeTest implements RewriteTest {

  @Override
  public void defaults(RecipeSpec spec) {
    spec.recipe(new JavaRDDUnionRecipe())
        .parser(CookbookFactory.buildParser(CookbookEnum.SPARK));
  }

  @Test
  void testRewriteFilter() {
    rewriteRun(
        java(
            """
                  import org.apache.spark.api.java.JavaRDD;
                  
                  class Convert {
                    public void run(JavaRDD<String> rdd1, JavaRDD<String> rdd2) {
                      JavaRDD<String> filtered = rdd1.union(rdd2);
                    }
                  }
                """,
            """
                  import org.apache.beam.sdk.transforms.Flatten;
                  import org.apache.beam.sdk.values.PCollectionList;
                  import org.apache.spark.api.java.JavaRDD;
                  
                  class Convert {
                    public void run(JavaRDD<String> rdd1, JavaRDD<String> rdd2) {
                      JavaRDD<String> filtered = PCollectionList.of(rdd1).and(rdd2).apply("Flatten", Flatten.pCollections());
                    }
                  }
                """
        )
    );
  }

}