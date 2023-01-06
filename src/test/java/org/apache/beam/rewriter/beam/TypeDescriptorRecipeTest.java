package org.apache.beam.rewriter.beam;

import static org.openrewrite.java.Assertions.java;

import org.apache.beam.rewriter.common.CookbookFactory;
import org.junit.jupiter.api.Test;
import org.openrewrite.test.RecipeSpec;
import org.openrewrite.test.RewriteTest;

class TypeDescriptorRecipeTest implements RewriteTest {

  @Override
  public void defaults(RecipeSpec spec) {
    spec.recipe(new TypeDescriptorRecipe())
        .parser(CookbookFactory.beamBuilder());
  }

  @Test
  void testRewriteDescriptors() {
    rewriteRun(
        java(
            """
                  import org.apache.beam.sdk.values.Row;
                  import org.apache.beam.sdk.values.TypeDescriptor;
                  
                  class Convert {
                    public void run() {
                      TypeDescriptor<String> typeString = TypeDescriptor.of(String.class);
                      TypeDescriptor<Integer> typeInt = TypeDescriptor.of(Integer.class);
                      TypeDescriptor<Row> typeRow = TypeDescriptor.of(Row.class);
                    }
                  }
                """,
            """
                  import org.apache.beam.sdk.values.Row;
                  import org.apache.beam.sdk.values.TypeDescriptor;
                  import org.apache.beam.sdk.values.TypeDescriptors;
                  
                  class Convert {
                    public void run() {
                      TypeDescriptor<String> typeString = TypeDescriptors.strings();
                      TypeDescriptor<Integer> typeInt = TypeDescriptors.integers();
                      TypeDescriptor<Row> typeRow = TypeDescriptors.rows();
                    }
                  }
                """
        )
    );
  }

  @Test
  void testInlineDescriptor() {
    rewriteRun(
        java(
            """
                  import org.apache.beam.sdk.values.Row;
                  import org.apache.beam.sdk.values.TypeDescriptor;
                  
                  class Convert {
                    public void run() {
                      TypeDescriptor.of(String.class);
                    }
                  }
                """,
            """
                  import org.apache.beam.sdk.values.Row;
                  import org.apache.beam.sdk.values.TypeDescriptors;
                  
                  class Convert {
                    public void run() {
                      TypeDescriptors.strings();
                    }
                  }
                """
        )
    );

  }

}