package org.apache.beam.rewriter.spark;

import static org.openrewrite.java.Assertions.java;

import org.junit.jupiter.api.Test;
import org.openrewrite.java.JavaParser;
import org.openrewrite.test.RecipeSpec;
import org.openrewrite.test.RewriteTest;

class JavaRDDMapRecipeTest implements RewriteTest {

  @Override
  public void defaults(RecipeSpec spec) {
    spec.recipe(new JavaRDDMapRecipe())
        .parser(JavaParser.fromJavaVersion().classpath("spark", "beam", "scala"));
  }

  @Test
  void convertMap() {
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
                  import org.apache.spark.api.java.JavaRDD;
                  
                  class Convert {
                    public void run(JavaRDD<String> rdd) {
                      JavaRDD<String> filtered = rdd
                        .apply("Map", MapElements.via(word -> word.toLowerCase()));
                    }
                  }
                """
        )
    );
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
                  import org.apache.spark.api.java.JavaPairRDD;
                  import org.apache.spark.api.java.JavaRDD;
                  
                  class Convert {
                    public void run(JavaRDD<String> rdd) {
                      JavaPairRDD<String, Integer> filtered = rdd
                        .apply("MapPair", MapElements.via(word -> new Tuple2<>(word.toLowerCase(), 1)));
                    }
                  }
                """
        )
    );
  }

}