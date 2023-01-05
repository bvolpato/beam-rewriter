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
import org.openrewrite.java.MethodMatcher;
import org.openrewrite.java.search.UsesType;
import org.openrewrite.java.tree.J;
import org.openrewrite.java.tree.JavaType;
import org.openrewrite.java.tree.JavaType.Parameterized;

public class JavaRDDMapToPairRecipe extends Recipe {

  @Override
  public String getDisplayName() {
    return "Replaces JavaRDD MapToPair with a MapElements transform";
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
    return new UsesType<>("org.apache.spark.api.java.AbstractJavaRDDLike");
  }

  @Override
  public JavaIsoVisitor<ExecutionContext> getVisitor() {
    return new Visitor();
  }

  static class Visitor extends JavaIsoVisitor<ExecutionContext> {

    final MethodMatcher filterPairMatcher =
        new MethodMatcher("org.apache.spark.api.java.AbstractJavaRDDLike mapToPair(..)", false);

    @Override
    public J.MethodInvocation visitMethodInvocation(
        J.MethodInvocation method, ExecutionContext executionContext) {
      if (filterPairMatcher.matches(method)) {
        Parameterized parameterized = (Parameterized) method.getArguments().get(0).getType();
        String type1 = ((JavaType.Class) parameterized.getTypeParameters().get(1)).getClassName();
        String type2 = ((JavaType.Class) parameterized.getTypeParameters().get(2)).getClassName();

        J.MethodInvocation mi =
            method
                .withName(method.getName().withSimpleName("apply"))
                .withTemplate(
                    JavaTemplate.builder(
                            this::getCursor,
                            "#{any(PCollection)}.apply(\"MapToPair\", MapElements.into(TypeDescriptors.kvs(TypeDescriptor.of("
                                + type1
                                + ".class), TypeDescriptor.of("
                                + type2
                                + ".class))).via(#{any(SerializableFunction)}))")
                        .imports("org.apache.beam.sdk.transforms.MapElements")
                        .imports("org.apache.beam.sdk.transforms.SerializableFunction")
                        .imports("org.apache.beam.sdk.values.TypeDescriptor")
                        .imports("org.apache.beam.sdk.values.TypeDescriptors")
                        .javaParser(CookbookFactory.beamParser())
                        .build(),
                    method.getCoordinates().replaceMethod(),
                    method.getSelect(),
                    method.getArguments().get(0));
        maybeAddImport("org.apache.beam.sdk.transforms.MapElements");
        maybeAddImport("org.apache.beam.sdk.values.TypeDescriptor");
        maybeAddImport("org.apache.beam.sdk.values.TypeDescriptors");
        return mi;
      } else {
        return super.visitMethodInvocation(method, executionContext);
      }
    }
  }
}
