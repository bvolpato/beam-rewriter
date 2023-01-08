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
import org.openrewrite.java.search.UsesType;
import org.openrewrite.java.tree.J;
import org.openrewrite.java.tree.JavaType;
import org.openrewrite.java.tree.JavaType.Parameterized;

public class JavaRDDMapRecipe extends Recipe {

  @Override
  public String getDisplayName() {
    return "Replaces JavaRDD Map with a MapElements transform";
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
        new MethodMatcher("org.apache.spark.api.java.AbstractJavaRDDLike map(..)", false);

    @Override
    public J.MethodInvocation visitMethodInvocation(
        J.MethodInvocation method, ExecutionContext executionContext) {
      J.MethodInvocation mi = super.visitMethodInvocation(method, executionContext);

      if (filterMatcher.matches(mi)) {
        Parameterized parameterized = (Parameterized) mi.getArguments().get(0).getType();
        JavaType.Class typeClass = (JavaType.Class) parameterized.getTypeParameters().get(1);

        String type = typeClass.getClassName();
        mi =
            mi
                .withName(mi.getName().withSimpleName("apply"))
                .withTemplate(
                    JavaTemplate.builder(
                            this::getCursor,
                            "#{any(PCollection)}.apply(\"Map\", MapElements.into(TypeDescriptor.of("
                                + type
                                + ".class)).via(#{any(SerializableFunction)}))")
                        .imports("org.apache.beam.sdk.transforms.MapElements")
                        .imports("org.apache.beam.sdk.transforms.SerializableFunction")
                        .imports("org.apache.beam.sdk.values.TypeDescriptor")
                        .javaParser(CookbookFactory.beamParser())
                        .build(),
                    mi.getCoordinates().replaceMethod(),
                    mi.getSelect(),
                    mi.getArguments().get(0));
        maybeAddImport("org.apache.beam.sdk.transforms.MapElements");
        maybeAddImport("org.apache.beam.sdk.values.TypeDescriptor");
        maybeAddImport("org.apache.beam.sdk.values.TypeDescriptors");
        return mi;
      } else {
        return mi;
      }
    }
  }
}
