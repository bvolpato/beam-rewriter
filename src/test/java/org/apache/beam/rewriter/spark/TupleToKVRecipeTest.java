package org.apache.beam.rewriter.spark;

import static org.openrewrite.java.Assertions.java;

import org.junit.jupiter.api.Test;
import org.openrewrite.java.JavaParser;
import org.openrewrite.test.RecipeSpec;
import org.openrewrite.test.RewriteTest;

class TupleToKVRecipeTest implements RewriteTest {

  @Override
  public void defaults(RecipeSpec spec) {
    spec.recipe(new TupleToKVRecipe())
        .parser(JavaParser.fromJavaVersion().classpath("spark", "beam", "scala"));
  }

  @Test
  void convertFilter() {
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

}