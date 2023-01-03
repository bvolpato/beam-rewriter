package org.apache.beam.rewriter.spark;

import static org.openrewrite.java.Assertions.java;

import org.junit.jupiter.api.Test;
import org.openrewrite.java.JavaParser;
import org.openrewrite.test.RecipeSpec;
import org.openrewrite.test.RewriteTest;

class JavaRDDtoPCollectionRecipeTest implements RewriteTest {

  @Override
  public void defaults(RecipeSpec spec) {
    spec.recipe(new JavaRDDtoPCollectionRecipe())
        .parser(JavaParser.fromJavaVersion().classpath("spark", "beam"));
  }

  @Test
  void testRewriteRDD() {
    rewriteRun(
        java(
            """
                  import org.apache.spark.api.java.JavaRDD;
                  
                  class Convert {
                    public void run(JavaRDD<String> rdd) {
                      JavaRDD<String> filtered = rdd;
                    }
                  }
                """,
            """
                import org.apache.beam.sdk.values.PCollection;
                                                                       
                class Convert {
                  public void run(PCollection<String> rdd) {
                    PCollection<String> filtered = rdd;
                  }
                }
                """
        )
    );
  }

}