package org.apache.beam.rewriter.spark;

import static org.openrewrite.java.Assertions.java;

import org.apache.beam.rewriter.common.CookbookEnum;
import org.apache.beam.rewriter.common.CookbookFactory;
import org.openrewrite.test.RecipeSpec;
import org.openrewrite.test.RewriteTest;

class JavaPairRDDSaveAsTextFileRecipeTest implements RewriteTest {

  @Override
  public void defaults(RecipeSpec spec) {
    spec.recipe(new JavaPairRDDSaveAsTextFileRecipe())
        .parser(CookbookFactory.buildParser(CookbookEnum.SPARK));
  }

  //@Test
  void testRewriteTextFilePair() {
    rewriteRun(java("""
          import org.apache.spark.api.java.JavaPairRDD;
          
          class Convert {
              public void run(JavaPairRDD<String, Integer> rdd) {
                  rdd.saveAsTextFile("/tmp/shakespeare.txt");
              }
          }
        """, """
         import org.apache.beam.sdk.io.TextIO;
         import org.apache.beam.sdk.transforms.MapElements;
         import org.apache.beam.sdk.values.TypeDescriptors;
         import org.apache.spark.api.java.JavaPairRDD;

         class Convert {
             public void run(JavaPairRDD<String, Integer> rdd) {
                 rdd.apply("KVToString", MapElements.into(TypeDescriptors.strings()).via(kv -> String.valueOf(kv))).apply("WriteTextFile", TextIO.write().to("/tmp/shakespeare.txt"));
             }
         }
        """));
  }

}