package org.apache.beam.rewriter.beam;

import com.google.common.collect.ImmutableSet;
import java.time.Duration;
import java.util.Set;
import org.apache.beam.rewriter.common.CookbookFactory;
import org.openrewrite.ExecutionContext;
import org.openrewrite.Recipe;
import org.openrewrite.TreeVisitor;
import org.openrewrite.java.JavaIsoVisitor;
import org.openrewrite.java.JavaTemplate;
import org.openrewrite.java.search.UsesType;
import org.openrewrite.java.tree.J;

public class AddMissingPipelineRunRecipe extends Recipe {

  @Override
  public String getDisplayName() {
    return "Add missing pipeline run() call";
  }

  @Override
  public String getDescription() {
    return getDisplayName() + ".";
  }

  @Override
  public Set<String> getTags() {
    return ImmutableSet.of();
  }

  @Override
  public Duration getEstimatedEffortPerOccurrence() {
    return Duration.ofMinutes(2);
  }

  @Override
  protected TreeVisitor<?, ExecutionContext> getSingleSourceApplicableTest() {
    return new UsesType<>("org.apache.beam.sdk.Pipeline");
  }

  @Override
  public JavaIsoVisitor<ExecutionContext> getVisitor() {
    return new Visitor();
  }

  static class Visitor extends JavaIsoVisitor<ExecutionContext> {

    @Override
    public J.Block visitBlock(J.Block m, ExecutionContext executionContext) {
      m = super.visitBlock(m, executionContext);

      // TODO: how to do it better? recurse with visitor + signaling
      if (m.getStatements().stream()
              .anyMatch(s -> s.toString().contains("pipeline = Pipeline.create"))
          && m.getStatements().stream().noneMatch(s -> s.toString().contains("pipeline.run()"))) {

        m =
            m.withTemplate(
                JavaTemplate.builder(this::getCursor, "pipeline.run();")
                    .javaParser(CookbookFactory.beamParser())
                    .build(),
                m.getCoordinates().lastStatement());
      }

      return m;
    }
  }
}
