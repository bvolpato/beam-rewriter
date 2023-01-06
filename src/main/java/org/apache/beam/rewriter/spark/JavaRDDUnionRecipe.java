package org.apache.beam.rewriter.spark;

import com.google.common.collect.ImmutableSet;
import java.time.Duration;
import java.util.Set;
import org.apache.beam.rewriter.common.CookbookFactory;
import org.openrewrite.ExecutionContext;
import org.openrewrite.Recipe;
import org.openrewrite.TreeVisitor;
import org.openrewrite.java.JavaIsoVisitor;
import org.openrewrite.java.JavaTemplate;
import org.openrewrite.java.JavaVisitor;
import org.openrewrite.java.MethodMatcher;
import org.openrewrite.java.search.UsesType;
import org.openrewrite.java.tree.J;

public class JavaRDDUnionRecipe extends Recipe {

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
    return new UsesType<>("org.apache.spark.api.java.JavaRDD");
  }

  @Override
  public JavaVisitor<ExecutionContext> getVisitor() {
    return new Visitor();
  }

  static class Visitor extends JavaVisitor<ExecutionContext> {

    final MethodMatcher filterMatcher =
        new MethodMatcher(
            "org.apache.spark.api.java.JavaRDD union(org.apache.spark.api.java.JavaRDD)",
            false);

    @Override
    public J visitMethodInvocation(
        J.MethodInvocation method, ExecutionContext executionContext) {
      if (filterMatcher.matches(method)) {
        System.out.println("Method1: " + method.getMethodType());

        J mi =
            method
                .withName(method.getName().withSimpleName("of"))
                .withTemplate(
                    JavaTemplate.builder(
                            this::getCursor,
                            "PCollectionList.of(#{any(PCollection)}).and(#{any(PCollection)}).apply(\"Flatten\", Flatten.pCollections()))")
                        .imports("org.apache.beam.sdk.transforms.Flatten")
                        .imports("org.apache.beam.sdk.values.PCollection")
                        .imports("org.apache.beam.sdk.values.PCollectionList")
                        .javaParser(CookbookFactory.beamParser())
                        .build(),
                    method.getCoordinates().replace(),
                    method.getSelect(),
                    method.getArguments().get(0));

        System.out.println("Method2: " + mi);

        maybeAddImport("org.apache.beam.sdk.transforms.Flatten");
        maybeAddImport("org.apache.beam.sdk.values.PCollection");
        maybeAddImport("org.apache.beam.sdk.values.PCollectionList");
        return mi;
      } else {
        return super.visitMethodInvocation(method, executionContext);
      }
    }
  }
}
