package org.apache.beam.rewriter.spark;

import static org.openrewrite.java.Assertions.java;

import org.junit.jupiter.api.Test;
import org.openrewrite.java.JavaParser;
import org.openrewrite.test.RecipeSpec;
import org.openrewrite.test.RewriteTest;

class JavaRDDFilterRecipeTest implements RewriteTest {

  @Override
  public void defaults(RecipeSpec spec) {
    spec.recipe(new JavaRDDFilterRecipe())
        .parser(JavaParser.fromJavaVersion().classpath("spark", "beam"));
  }

  @Test
  void convertFilter() {
    rewriteRun(
        java(
            """
                  import org.apache.spark.api.java.JavaRDD;
                  
                  class Convert {
                    public void run(JavaRDD<String> rdd) {
                      JavaRDD<String> filtered = rdd
                        .filter(word -> word.length() > 1);
                    }
                  }
                """,
            """
                  import org.apache.beam.sdk.transforms.Filter;
                  import org.apache.spark.api.java.JavaRDD;
                  
                  class Convert {
                    public void run(JavaRDD<String> rdd) {
                      JavaRDD<String> filtered = rdd
                        .apply("Filter", Filter.by(word -> word.length() > 1));
                    }
                  }
                """
        )
    );
  }

}