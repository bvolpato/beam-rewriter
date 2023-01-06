package org.apache.beam.rewriter.spark;

import static org.openrewrite.java.Assertions.java;

import org.apache.beam.rewriter.common.CookbookEnum;
import org.apache.beam.rewriter.common.CookbookFactory;
import org.junit.jupiter.api.Test;
import org.openrewrite.test.RecipeSpec;
import org.openrewrite.test.RewriteTest;

class JavaRDDFlatMapRecipeTest implements RewriteTest {

  @Override
  public void defaults(RecipeSpec spec) {
    spec.recipe(new JavaRDDFlatMapRecipe())
        .parser(CookbookFactory.buildParser(CookbookEnum.SPARK));
  }

  @Test
  void testRewriteMap() {
    rewriteRun(
        java(
            """
                  import java.util.Arrays;
                  import org.apache.spark.api.java.JavaRDD;
                                   
                  class Convert {
                    public void run(JavaRDD<String> rdd) {
                      JavaRDD<String> filtered = rdd
                        .flatMap(line -> Arrays.stream(line.split(" ")).iterator());
                    }
                  }
                """,
            """
                  import java.util.Arrays;
                  
                  import org.apache.beam.sdk.transforms.FlatMapElements;
                  import org.apache.beam.sdk.values.TypeDescriptor;
                  import org.apache.spark.api.java.JavaRDD;
                  
                  class Convert {
                    public void run(JavaRDD<String> rdd) {
                      JavaRDD<String> filtered = rdd
                        .apply("FlatMap", FlatMapElements.into(TypeDescriptor.of(String.class)).via(line -> Arrays.asList(line.split(" "))));
                    }
                  }
                """
        )
    );
  }

}