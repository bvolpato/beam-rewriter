package org.apache.beam.rewriter.spark;

import com.google.common.collect.ImmutableSet;
import java.time.Duration;
import java.util.Set;
import org.apache.beam.rewriter.common.CookbookFactory;
import org.apache.beam.rewriter.common.UsesPackage;
import org.openrewrite.ExecutionContext;
import org.openrewrite.Recipe;
import org.openrewrite.TreeVisitor;
import org.openrewrite.java.JavaIsoVisitor;
import org.openrewrite.java.JavaTemplate;
import org.openrewrite.java.MethodMatcher;
import org.openrewrite.java.tree.J;

/**
 * See {@link #getDescription()}.
 */
public class JavaRDDFilterRecipe extends Recipe {

  @Override
  public String getDisplayName() {
    return "Replaces JavaRDD Filter with a Filter transform";
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
    return new UsesPackage<>("org.apache.spark.api.java");
  }

  @Override
  public JavaIsoVisitor<ExecutionContext> getVisitor() {
    return new Visitor();
  }

  static class Visitor extends JavaIsoVisitor<ExecutionContext> {

    final MethodMatcher filterMatcher =
        new MethodMatcher("org.apache.spark.api.java.JavaRDD filter(..)", false);
    final MethodMatcher filterPairMatcher =
        new MethodMatcher("org.apache.spark.api.java.JavaPairRDD filter(..)", false);

    @Override
    public J.MethodInvocation visitMethodInvocation(
        J.MethodInvocation mi, ExecutionContext executionContext) {
      mi = super.visitMethodInvocation(mi, executionContext);

      if (filterMatcher.matches(mi) || filterPairMatcher.matches(mi)) {
        mi =
            mi.withName(mi.getName().withSimpleName("apply"))
                .withTemplate(
                    JavaTemplate.builder(
                            this::getCursor,
                            "#{any(PCollection)}.apply(\"Filter\", Filter.by(#{any(SerializableFunction)}))")
                        .imports("org.apache.beam.sdk.transforms.Filter")
                        .imports("org.apache.beam.sdk.transforms.SerializableFunction")
                        .javaParser(CookbookFactory.beamParser())
                        .build(),
                    mi.getCoordinates().replaceMethod(),
                    mi.getSelect(),
                    mi.getArguments().get(0));
        maybeAddImport("org.apache.beam.sdk.transforms.Filter");
      }

      return mi;
    }
  }
}
