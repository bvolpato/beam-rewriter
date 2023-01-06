package org.apache.beam.rewriter.spark;

import com.google.common.collect.ImmutableSet;
import java.time.Duration;
import java.util.Set;
import org.apache.beam.rewriter.common.CookbookFactory;
import org.openrewrite.ExecutionContext;
import org.openrewrite.Recipe;
import org.openrewrite.TreeVisitor;
import org.openrewrite.java.JavaIsoVisitor;
import org.openrewrite.java.JavaParser;
import org.openrewrite.java.JavaTemplate;
import org.openrewrite.java.MethodMatcher;
import org.openrewrite.java.search.UsesType;
import org.openrewrite.java.tree.J;

public class SparkContextTextFileRecipe extends Recipe {

  @Override
  public String getDisplayName() {
    return "Replaces SparkContext `textFile` with a TextIO transform";
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

    final MethodMatcher filterMatcher =
        new MethodMatcher(
            "org.apache.spark.api.java.JavaSparkContext textFile(java.lang.String)", false);

    @Override
    public J.MethodInvocation visitMethodInvocation(
        J.MethodInvocation method, ExecutionContext executionContext) {
      if (filterMatcher.matches(method)) {
        J.MethodInvocation mi =
            method
                .withName(method.getName().withSimpleName("apply"))
                .withTemplate(
                    JavaTemplate.builder(
                            this::getCursor,
                            "#{any(Pipeline)}.apply(\"ReadTextFile\", TextIO.read().from(#{any(java.lang.String)}))")
                        .imports("org.apache.beam.sdk.io.TextIO")
                        .javaParser(CookbookFactory.beamParser())
                        .build(),
                    method.getCoordinates().replaceMethod(),
                    method.getSelect(),
                    method.getArguments().get(0));
        maybeAddImport("org.apache.beam.sdk.io.TextIO");
        return mi;
      } else {
        return super.visitMethodInvocation(method, executionContext);
      }
    }
  }
}
