package org.apache.beam.rewriter.spark;

import static org.openrewrite.java.Assertions.java;

import org.apache.beam.rewriter.common.CookbookEnum;
import org.apache.beam.rewriter.common.CookbookFactory;
import org.junit.jupiter.api.Test;
import org.openrewrite.test.RecipeSpec;
import org.openrewrite.test.RewriteTest;

class SparkContextToPipelineRecipeTest implements RewriteTest {

  @Override
  public void defaults(RecipeSpec spec) {

    spec.recipe(new SparkContextToPipelineRecipe())
        .parser(CookbookFactory.buildParser(CookbookEnum.SPARK));

  }

  @Test
  void testRewriteSparkContext() {
    rewriteRun(java("""
          import org.apache.spark.api.java.JavaRDD;
          import org.apache.spark.api.java.JavaSparkContext;
          
          class Convert {
              public void run(JavaSparkContext sparkContext) {
                  JavaRDD<String> rdd = sparkContext.textFile("gs://beam-samples/shakespeare.txt");
              }
          }
        """, """
         import org.apache.beam.sdk.Pipeline;
         import org.apache.spark.api.java.JavaRDD;
          
         class Convert {
             public void run(Pipeline pipeline) {
                 JavaRDD<String> rdd = pipeline.textFile("gs://beam-samples/shakespeare.txt");
             }
         }
        """));
  }
}