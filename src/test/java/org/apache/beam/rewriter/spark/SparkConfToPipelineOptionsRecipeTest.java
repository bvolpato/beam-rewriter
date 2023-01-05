package org.apache.beam.rewriter.spark;

import static org.openrewrite.java.Assertions.java;

import org.apache.beam.rewriter.common.CookbookEnum;
import org.apache.beam.rewriter.common.CookbookFactory;
import org.junit.jupiter.api.Test;
import org.openrewrite.test.RecipeSpec;
import org.openrewrite.test.RewriteTest;

class SparkConfToPipelineOptionsRecipeTest implements RewriteTest {

  @Override
  public void defaults(RecipeSpec spec) {
    spec.recipe(new SparkConfToPipelineOptionsRecipe())
        .parser(CookbookFactory.buildParser(CookbookEnum.SPARK));
  }

  @Test
  void testRewriteSparkContext() {
    rewriteRun(java("""
        package com.company.spark;
                
        import org.apache.spark.SparkConf;
                
        public class WordCounter {
                
          public static void main(String[] args) {
                
            SparkConf sparkConf = new SparkConf()
                .setMaster("local")
                .setAppName("WordCount Sample")
                .set("spark.driver.bindAddress", "127.0.0.1")
                .set("spark.driver.host", "127.0.0.1");

          }
        }
        """, """
        package com.company.spark;
                
        import org.apache.beam.sdk.options.PipelineOptions;
        import org.apache.beam.sdk.options.PipelineOptionsFactory;
                                                     
        public class WordCounter {
                
          public static void main(String[] args) {
            
            PipelineOptions pipelineOptions = PipelineOptionsFactory.create();

          }
        }
        """));
  }
}