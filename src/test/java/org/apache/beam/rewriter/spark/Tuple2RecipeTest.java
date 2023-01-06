package org.apache.beam.rewriter.spark;

import static org.openrewrite.java.Assertions.java;

import org.apache.beam.rewriter.common.CookbookEnum;
import org.apache.beam.rewriter.common.CookbookFactory;
import org.apache.beam.rewriter.common.DebuggingRecipe;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.junit.jupiter.api.Test;
import org.openrewrite.test.RecipeSpec;
import org.openrewrite.test.RewriteTest;

class Tuple2RecipeTest implements RewriteTest {

  @Override
  public void defaults(RecipeSpec spec) {
    spec.recipe(new Tuple2Recipe())
        .parser(CookbookFactory.buildParser(CookbookEnum.SPARK));
  }

  @Test
  void testRewriteTupleToKV() {
    rewriteRun(
        java(
            """
                  import scala.Tuple2;
                  
                  class Convert {
                    public void run() {
                      Tuple2<String, Integer> maps = new Tuple2<>("a", 1);
                    }
                  }
                """,
            """
                  import org.apache.beam.sdk.values.KV;
                  
                  class Convert {
                    public void run() {
                      KV<String, Integer> maps = KV.of("a", 1);
                    }
                  }
                """
        )
    );
  }

    @Test
    void testRewriteTupleToKVFields() {
      // JavaPairRDD<String, Integer> data = null;
      // JavaRDD<String> strings = data.map(t2 -> t2._1 + "," + t2._2);

      rewriteRun(
          java(
              """
                    import org.apache.spark.api.java.JavaPairRDD;
                    import org.apache.spark.api.java.JavaRDD;
                    import scala.Tuple2;
                    
                    class Convert {
                      public void run(JavaPairRDD<String, Integer> data) {
                        JavaRDD<String> strings = data.map(t2 -> t2._1 + "," + t2._2);
                      }
                    }
                  """,
              """
                    import org.apache.spark.api.java.JavaPairRDD;
                    import org.apache.spark.api.java.JavaRDD;

                    class Convert {
                      public void run(JavaPairRDD<String, Integer> data) {
                        JavaRDD<String> strings = data.map(t2 -> t2.getKey() + "," + t2.getValue());
                      }
                    }
                  """
          )
      );
  }

}