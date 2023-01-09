package org.apache.beam.rewriter.flink;

import com.google.common.collect.ImmutableSet;
import java.time.Duration;
import java.util.Set;
import org.apache.beam.rewriter.common.UsesPackage;
import org.openrewrite.ExecutionContext;
import org.openrewrite.Recipe;
import org.openrewrite.TreeVisitor;
import org.openrewrite.java.ChangeType;
import org.openrewrite.java.JavaIsoVisitor;
import org.openrewrite.java.tree.J;

/** See {@link #getDescription()}. */
public class DataStreamtoPCollectionRecipe extends Recipe {

  @Override
  public String getDisplayName() {
    return "Convert Flink DataStream to Beam PCollection";
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
    return new UsesPackage<>("org.apache.flink.streaming.api");
  }

  @Override
  public JavaIsoVisitor<ExecutionContext> getVisitor() {
    return new Visitor();
  }

  static class Visitor extends JavaIsoVisitor<ExecutionContext> {

    @Override
    public J.CompilationUnit visitCompilationUnit(J.CompilationUnit cu, ExecutionContext ctx) {
      J.CompilationUnit c = super.visitCompilationUnit(cu, ctx);
      doAfterVisit(
          new ChangeType(
              "org.apache.flink.streaming.api.datastream.DataStream",
              "org.apache.beam.sdk.values.PCollection",
              true));
      return c;
    }
  }
}
