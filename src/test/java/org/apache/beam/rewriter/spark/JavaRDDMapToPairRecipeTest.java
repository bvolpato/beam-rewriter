package org.apache.beam.rewriter.spark;

import static org.openrewrite.java.Assertions.java;

import org.apache.beam.rewriter.common.CookbookEnum;
import org.apache.beam.rewriter.common.CookbookFactory;
import org.junit.jupiter.api.Test;
import org.openrewrite.test.RecipeSpec;
import org.openrewrite.test.RewriteTest;

class JavaRDDMapToPairRecipeTest implements RewriteTest {

  @Override
  public void defaults(RecipeSpec spec) {
    spec.recipe(new JavaRDDMapToPairRecipe())
        .parser(CookbookFactory.buildParser(CookbookEnum.SPARK));
  }

  @Test
  void convertMapToPair() {
    rewriteRun(
        java(
            """
                  import org.apache.spark.api.java.JavaPairRDD;
                  import org.apache.spark.api.java.JavaRDD;
                  
                  class Convert {
                    public void run(JavaRDD<String> rdd) {
                      JavaPairRDD<String, Integer> filtered = rdd
                        .mapToPair(word -> new Tuple2<>(word.toLowerCase(), 1));
                    }
                  }
                """,
            """
                  import org.apache.beam.sdk.transforms.MapElements;
                  import org.apache.beam.sdk.values.TypeDescriptor;
                  import org.apache.beam.sdk.values.TypeDescriptors;
                  import org.apache.spark.api.java.JavaPairRDD;
                  import org.apache.spark.api.java.JavaRDD;
                  
                  class Convert {
                    public void run(JavaRDD<String> rdd) {
                      JavaPairRDD<String, Integer> filtered = rdd
                        .apply("MapToPair", MapElements.into(TypeDescriptors.kvs(TypeDescriptor.of(String.class), TypeDescriptor.of(Integer.class))).via(word -> new Tuple2<>(word.toLowerCase(), 1)));
                    }
                  }
                """
        )
    );
  }

}