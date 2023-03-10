package org.apache.beam.rewriter.spark;

import static org.openrewrite.java.Assertions.java;

import org.apache.beam.rewriter.common.CookbookEnum;
import org.apache.beam.rewriter.common.CookbookFactory;
import org.junit.jupiter.api.Test;
import org.openrewrite.test.RecipeSpec;
import org.openrewrite.test.RewriteTest;

class JavaRDDMapRecipeTest implements RewriteTest {

  @Override
  public void defaults(RecipeSpec spec) {
    spec.recipe(new JavaRDDMapRecipe())
        .parser(CookbookFactory.buildParser(CookbookEnum.SPARK));
  }

  @Test
  void testRewriteMap() {
    rewriteRun(
        java(
            """
                  import org.apache.spark.api.java.JavaRDD;
                  
                  class Convert {
                    public void run(JavaRDD<String> rdd) {
                      JavaRDD<String> filtered = rdd
                        .map(word -> word.toLowerCase());
                    }
                  }
                """,
            """
                  import org.apache.beam.sdk.transforms.MapElements;
                  import org.apache.beam.sdk.values.TypeDescriptor;
                  import org.apache.spark.api.java.JavaRDD;
                  
                  class Convert {
                    public void run(JavaRDD<String> rdd) {
                      JavaRDD<String> filtered = rdd
                        .apply("Map", MapElements.into(TypeDescriptor.of(String.class)).via(word -> word.toLowerCase()));
                    }
                  }
                """
        )
    );
  }

}