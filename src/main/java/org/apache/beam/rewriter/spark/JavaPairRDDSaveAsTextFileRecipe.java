package org.apache.beam.rewriter.spark;

import com.google.common.collect.ImmutableSet;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;
import org.apache.beam.rewriter.common.CookbookFactory;
import org.openrewrite.Cursor;
import org.openrewrite.ExecutionContext;
import org.openrewrite.Recipe;
import org.openrewrite.TreeVisitor;
import org.openrewrite.java.JavaIsoVisitor;
import org.openrewrite.java.JavaTemplate;
import org.openrewrite.java.JavaVisitor;
import org.openrewrite.java.MethodMatcher;
import org.openrewrite.java.search.UsesType;
import org.openrewrite.java.tree.Expression;
import org.openrewrite.java.tree.J;
import org.openrewrite.java.tree.JavaType;
import org.openrewrite.java.tree.JavaType.Parameterized;
import org.openrewrite.java.tree.JavaType.ShallowClass;

public class JavaPairRDDSaveAsTextFileRecipe extends Recipe {

  @Override
  public String getDisplayName() {
    return "Replaces JavaPairRDD `saveAsTextFile` with a TextIO transform";
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
    return new UsesType<>("org.apache.spark.api.java.JavaPairRDD");
  }

  @Override
  public JavaVisitor<ExecutionContext> getVisitor() {
    return new Visitor();
  }

  static class Visitor extends JavaVisitor<ExecutionContext> {

    final MethodMatcher filterMatcher =
        new MethodMatcher(
            "org.apache.spark.api.java.AbstractJavaRDDLike saveAsTextFile(..)", true);

    @Override
    public J visitMethodInvocation(
        J.MethodInvocation method, ExecutionContext executionContext) {
      if (filterMatcher.matches(method)) {
        System.out.println("Method1: " + method.getMethodType());

        JavaType.Parameterized rddClass = (JavaType.Parameterized) method.getSelect().getType();
        JavaType.Class kvType
            = ShallowClass.build("org.apache.beam.sdk.values.KV")
            .withTypeParameters(
                List.of(
                    rddClass.getTypeParameters().get(0), rddClass.getTypeParameters().get(1)));

        J result =
            method
                .withName(method.getName().withSimpleName("apply"))
                .withTemplate(
                    JavaTemplate.builder(
                            this::getCursor,
                            "#{any(PCollection)}.apply(\"KVToString\", MapElements.into(TypeDescriptors.strings()).via(kv -> String.valueOf(kv))).apply(\"WriteTextFile\", TextIO.write().to(#{any()}))")
                        .imports("org.apache.beam.sdk.io.TextIO")
                        .imports("org.apache.beam.sdk.transforms.MapElements")
                        .imports("org.apache.beam.sdk.values.KV")
                        .imports("org.apache.beam.sdk.values.PCollection")
                        .imports("org.apache.beam.sdk.values.TypeDescriptors")
                        .javaParser(CookbookFactory.beamParser())
                        .build(),
                    method.getCoordinates().replace(),
                    method.getSelect(),
                    method.getArguments().get(0));

        maybeAddImport("org.apache.beam.sdk.io.TextIO");
        maybeAddImport("org.apache.beam.sdk.transforms.MapElements");
        maybeAddImport("org.apache.beam.sdk.values.KV");
        maybeAddImport("org.apache.beam.sdk.values.TypeDescriptors");
        return result;
      } else {
        return super.visitMethodInvocation(method, executionContext);
      }
    }
  }
}
