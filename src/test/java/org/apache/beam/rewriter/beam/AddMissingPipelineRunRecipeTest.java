package org.apache.beam.rewriter.beam;

import static org.openrewrite.java.Assertions.java;

import org.apache.beam.rewriter.common.CookbookFactory;
import org.junit.jupiter.api.Test;
import org.openrewrite.test.RecipeSpec;
import org.openrewrite.test.RewriteTest;

class AddMissingPipelineRunRecipeTest implements RewriteTest {

  @Override
  public void defaults(RecipeSpec spec) {
    spec.recipe(new AddMissingPipelineRunRecipe())
        .parser(CookbookFactory.beamBuilder());
  }

  @Test
  void testRewriteWithOptions() {
    rewriteRun(
        java(
            """
                  import org.apache.beam.sdk.Pipeline;
                  import org.apache.beam.sdk.options.PipelineOptionsFactory;
                  
                  class Convert {
                    public void run() {
                      Pipeline pipeline = Pipeline.create(PipelineOptionsFactory.create());
                    }
                  }
                """,
            """
                  import org.apache.beam.sdk.Pipeline;
                  import org.apache.beam.sdk.options.PipelineOptionsFactory;
                  
                  class Convert {
                    public void run() {
                      Pipeline pipeline = Pipeline.create(PipelineOptionsFactory.create());
                        pipeline.run();
                    }
                  }
                """
        )
    );
  }

  @Test
  void testRewriteWithoutOptions() {
    rewriteRun(
        java(
            """
                  import org.apache.beam.sdk.Pipeline;
                  
                  class Convert {
                    public void run() {
                      Pipeline pipeline = Pipeline.create();
                    }
                  }
                """,
            """
                  import org.apache.beam.sdk.Pipeline;
                  
                  class Convert {
                    public void run() {
                      Pipeline pipeline = Pipeline.create();
                        pipeline.run();
                    }
                  }
                """
        )
    );
  }


}