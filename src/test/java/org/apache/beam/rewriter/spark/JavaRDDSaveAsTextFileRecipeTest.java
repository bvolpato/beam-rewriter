package org.apache.beam.rewriter.spark;

import static org.openrewrite.java.Assertions.java;

import org.apache.beam.rewriter.common.CookbookEnum;
import org.apache.beam.rewriter.common.CookbookFactory;
import org.junit.jupiter.api.Test;
import org.openrewrite.test.RecipeSpec;
import org.openrewrite.test.RewriteTest;

class JavaRDDSaveAsTextFileRecipeTest implements RewriteTest {

  @Override
  public void defaults(RecipeSpec spec) {
    spec.recipe(new JavaRDDSaveAsTextFileRecipe())
        .parser(CookbookFactory.buildParser(CookbookEnum.SPARK));
  }

  @Test
  void testRewriteTextFile() {
    rewriteRun(java("""
          import org.apache.spark.api.java.JavaRDD;
          
          class Convert {
              public void run(JavaRDD<String> rdd) {
                  rdd.saveAsTextFile("/tmp/shakespeare.txt");
              }
          }
        """, """
         import org.apache.beam.sdk.io.TextIO;
         import org.apache.spark.api.java.JavaRDD;

         class Convert {
             public void run(JavaRDD<String> rdd) {
                 rdd.apply("WriteTextFile", TextIO.write().to("/tmp/shakespeare.txt"));
             }
         }
        """));
  }

}