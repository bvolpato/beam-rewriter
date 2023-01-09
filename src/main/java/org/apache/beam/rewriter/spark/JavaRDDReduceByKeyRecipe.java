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
import org.openrewrite.java.JavaParser;
import org.openrewrite.java.JavaTemplate;
import org.openrewrite.java.MethodMatcher;
import org.openrewrite.java.search.UsesType;
import org.openrewrite.java.tree.J;

/**
 * See {@link #getDescription()}.
 */
public class JavaRDDReduceByKeyRecipe extends Recipe {

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
        new MethodMatcher("org.apache.spark.api.java.JavaPairRDD reduceByKey(..)", true);

    @Override
    public J.MethodInvocation visitMethodInvocation(
        J.MethodInvocation methodZ, ExecutionContext executionContext) {
      J.MethodInvocation mi = super.visitMethodInvocation(methodZ, executionContext);
      if (filterMatcher.matches(mi)) {
        mi =
            mi
                .withName(mi.getName().withSimpleName("apply"))
                .withTemplate(
                    JavaTemplate.builder(
                            this::getCursor,
                            "#{any(PCollection)}.apply(\"CombinePerKey\", Combine.perKey(#{any(SerializableFunction)}))")
                        .imports("org.apache.beam.sdk.transforms.Combine")
                        .imports("org.apache.beam.sdk.transforms.SerializableFunction")
                        .javaParser(CookbookFactory.beamParser())
                        .build(),
                    mi.getCoordinates().replaceMethod(),
                    mi.getSelect(),
                    mi.getArguments().get(0));
        maybeAddImport("org.apache.beam.sdk.transforms.Combine");
        return mi;
      } else {
        return mi;
      }
    }
  }
}
