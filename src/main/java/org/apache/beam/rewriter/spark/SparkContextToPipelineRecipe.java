package org.apache.beam.rewriter.spark;

import com.google.common.collect.ImmutableSet;
import java.time.Duration;
import java.util.Set;
import org.openrewrite.ExecutionContext;
import org.openrewrite.Recipe;
import org.openrewrite.TreeVisitor;
import org.openrewrite.java.ChangeType;
import org.openrewrite.java.JavaIsoVisitor;
import org.openrewrite.java.search.UsesType;
import org.openrewrite.java.tree.J;

public class SparkContextToPipelineRecipe extends Recipe {

  @Override
  public String getDisplayName() {
    return "Convert SparkContext to Beam Pipeline";
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
    return new UsesType<>("org.apache.spark.api.java.JavaSparkContext");
  }

  @Override
  public JavaIsoVisitor<ExecutionContext> getVisitor() {
    return new Visitor();
  }

  static class Visitor extends JavaIsoVisitor<ExecutionContext> {

    @Override
    public J.CompilationUnit visitCompilationUnit(J.CompilationUnit cu, ExecutionContext ctx) {
      J.CompilationUnit c = super.visitCompilationUnit(cu, ctx);
      doAfterVisit(new ChangeType("org.apache.spark.api.java.JavaSparkContext",
          "org.apache.beam.sdk.Pipeline", true));
      return c;
    }

    @Override
    public J.Identifier visitIdentifier(J.Identifier identifier, ExecutionContext ctx) {
      identifier = super.visitIdentifier(identifier, ctx);
      if (identifier.getSimpleName().equals("sparkContext")) {
        return identifier.withSimpleName("pipeline");
      }
      return identifier;
    }

  }

}