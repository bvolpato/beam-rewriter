package org.apache.beam.rewriter.spark;

import static org.openrewrite.java.Assertions.java;

import org.apache.beam.rewriter.common.CookbookEnum;
import org.apache.beam.rewriter.common.CookbookFactory;
import org.junit.jupiter.api.Test;
import org.openrewrite.test.RecipeSpec;
import org.openrewrite.test.RewriteTest;

class JavaRDDForEachRecipeTest implements RewriteTest {

  @Override
  public void defaults(RecipeSpec spec) {
    spec.recipe(new JavaRDDForEachRecipe())
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
                      rdd.foreach(word -> word.toLowerCase());
                    }
                  }
                """,
            """
                  import org.apache.beam.sdk.transforms.MapElements;
                  import org.apache.beam.sdk.values.TypeDescriptors;
                  import org.apache.spark.api.java.JavaRDD;
                  
                  class Convert {
                    public void run(JavaRDD<String> rdd) {
                      rdd.apply("ForEach", MapElements.into(TypeDescriptors.voids()).via(word -> {
                            word.toLowerCase();
                            return null;
                        }));
                    }
                  }
                """
        )
    );
  }

}