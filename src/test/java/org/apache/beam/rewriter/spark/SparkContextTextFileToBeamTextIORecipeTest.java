package org.apache.beam.rewriter.spark;

import static org.openrewrite.java.Assertions.java;

import org.junit.jupiter.api.Test;
import org.openrewrite.java.JavaParser;
import org.openrewrite.test.RecipeSpec;
import org.openrewrite.test.RewriteTest;

class SparkContextTextFileToBeamTextIORecipeTest implements RewriteTest {

  @Override
  public void defaults(RecipeSpec spec) {

    spec.recipe(new SparkContextTextFileToBeamTextIORecipe())
        .parser(JavaParser.fromJavaVersion().classpath("beam", "spark"));

  }

  @Test
  void testRewriteTextFile() {
    rewriteRun(java("""
          import org.apache.spark.api.java.JavaRDD;
          import org.apache.spark.api.java.JavaSparkContext;
          
          class Convert {
              public void run(JavaSparkContext sparkContext) {
                  JavaRDD<String> rdd = sparkContext.textFile("gs://beam-samples/shakespeare.txt");
              }
          }
        """, """
         import org.apache.beam.sdk.io.TextIO;
         import org.apache.spark.api.java.JavaRDD;
         import org.apache.spark.api.java.JavaSparkContext;

         class Convert {
             public void run(JavaSparkContext sparkContext) {
                 JavaRDD<String> rdd = sparkContext.apply("ReadTextFile", TextIO.read().from("gs://beam-samples/shakespeare.txt"));
             }
         }
        """));
  }
}