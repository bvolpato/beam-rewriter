package org.apache.beam.rewriter.flink;

import static org.openrewrite.java.Assertions.java;

import org.apache.beam.rewriter.common.CookbookEnum;
import org.apache.beam.rewriter.common.CookbookFactory;
import org.apache.beam.rewriter.spark.JavaRDDtoPCollectionRecipe;
import org.junit.jupiter.api.Test;
import org.openrewrite.test.RecipeSpec;
import org.openrewrite.test.RewriteTest;

class DataStreamtoPCollectionRecipeTest implements RewriteTest {

  @Override
  public void defaults(RecipeSpec spec) {
    spec.recipe(new DataStreamtoPCollectionRecipe())
        .parser(CookbookFactory.buildParser(CookbookEnum.FLINK));
  }

  @Test
  void testRewriteRDD() {
    rewriteRun(
        java(
            """
                  import org.apache.flink.streaming.api.datastream.DataStream;
                  
                  class Convert {
                    public void run(DataStream<String> rdd) {
                      DataStream<String> filtered = rdd;
                    }
                  }
                """,
            """
                import org.apache.beam.sdk.values.PCollection;
                                                                       
                class Convert {
                  public void run(PCollection<String> rdd) {
                    PCollection<String> filtered = rdd;
                  }
                }
                """
        )
    );
  }

}