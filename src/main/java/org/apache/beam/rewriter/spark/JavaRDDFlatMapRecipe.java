package org.apache.beam.rewriter.spark;

import com.google.common.collect.ImmutableSet;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.beam.rewriter.common.CookbookFactory;
import org.apache.beam.rewriter.common.UsesPackage;
import org.openrewrite.ExecutionContext;
import org.openrewrite.Recipe;
import org.openrewrite.TreeVisitor;
import org.openrewrite.java.JavaTemplate;
import org.openrewrite.java.JavaVisitor;
import org.openrewrite.java.MethodMatcher;
import org.openrewrite.java.search.UsesType;
import org.openrewrite.java.tree.J;
import org.openrewrite.java.tree.JavaType;
import org.openrewrite.java.tree.JavaType.Parameterized;

public class JavaRDDFlatMapRecipe extends Recipe {

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
  public JavaVisitor<ExecutionContext> getVisitor() {
    return new Visitor();
  }

  static class Visitor extends JavaVisitor<ExecutionContext> {

    final MethodMatcher filterMatcher =
        new MethodMatcher("org.apache.spark.api.java.AbstractJavaRDDLike flatMap(..)", false);

    final AtomicBoolean insideFlat = new AtomicBoolean();

    @Override
    public J visitMethodInvocation(J.MethodInvocation methodZ, ExecutionContext executionContext) {
      J.MethodInvocation mi = (J.MethodInvocation) super.visitMethodInvocation(methodZ, executionContext);

      if (filterMatcher.matches(mi)) {
        Parameterized parameterized = (Parameterized) mi.getArguments().get(0).getType();
        JavaType.Class typeClass = (JavaType.Class) parameterized.getTypeParameters().get(1);

        String type = typeClass.getClassName();

        insideFlat.set(true);

        mi =
            mi
                .withName(mi.getName().withSimpleName("apply"))
                .withTemplate(
                    JavaTemplate.builder(
                            this::getCursor,
                            "#{any(PCollection)}.apply(\"FlatMap\", FlatMapElements.into(TypeDescriptor.of("
                                + type
                                + ".class)).via(#{any(SerializableFunction)}))")
                        .imports("org.apache.beam.sdk.transforms.FlatMapElements")
                        .imports("org.apache.beam.sdk.transforms.SerializableFunction")
                        .imports("org.apache.beam.sdk.values.TypeDescriptor")
                        .javaParser(CookbookFactory.beamParser())
                        .build(),
                    mi.getCoordinates().replaceMethod(),
                    mi.getSelect(),
                    visit(mi.getArguments().get(0), executionContext));

        insideFlat.set(false);

        maybeAddImport("org.apache.beam.sdk.transforms.FlatMapElements");
        maybeAddImport("org.apache.beam.sdk.values.TypeDescriptor");
        maybeAddImport("org.apache.beam.sdk.values.TypeDescriptors");
        return mi;
      } else if (insideFlat.get()) {

        // Drop any .iterator() that's returned
        if (new MethodMatcher("java.util.stream.BaseStream iterator()", true).matches(mi)) {
          return visit(mi.getSelect(), executionContext);
        }

        // Change any Arrays.stream to Arrays.asList
        if (new MethodMatcher("java.util.Arrays stream(..)", true).matches(mi)) {
          return mi.withName(mi.getName().withSimpleName("asList"));
        }
      }

      return super.visitMethodInvocation(mi, executionContext);
    }
  }
}
