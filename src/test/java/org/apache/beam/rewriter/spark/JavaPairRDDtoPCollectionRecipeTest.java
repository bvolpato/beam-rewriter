package org.apache.beam.rewriter.spark;

import static org.openrewrite.java.Assertions.java;

import org.apache.beam.rewriter.common.CookbookEnum;
import org.apache.beam.rewriter.common.CookbookFactory;
import org.junit.jupiter.api.Test;
import org.openrewrite.test.RecipeSpec;
import org.openrewrite.test.RewriteTest;

class JavaPairRDDtoPCollectionRecipeTest implements RewriteTest {

  @Override
  public void defaults(RecipeSpec spec) {
    spec.recipe(new JavaPairRDDtoPCollectionRecipe())
        .parser(CookbookFactory.buildParser(CookbookEnum.SPARK));
  }

  @Test
  void testRewritePairToPCollection() {
    rewriteRun(
        java(
            """
                  import org.apache.spark.api.java.JavaPairRDD;
                  
                  class Convert {
                    public void run(JavaPairRDD<String, Integer> rdd) {
                      JavaPairRDD<String, Integer> filtered = rdd;
                    }
                  }
                """,
            """
                import org.apache.beam.sdk.values.KV;
                import org.apache.beam.sdk.values.PCollection;
                                                                       
                class Convert {
                  public void run(PCollection<KV<String,Integer>> rdd) {
                    PCollection<KV<String,Integer>> filtered = rdd;
                  }
                }
                """
        )
    );
  }

}