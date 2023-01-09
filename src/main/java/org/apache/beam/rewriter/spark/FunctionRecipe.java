package org.apache.beam.rewriter.spark;

import com.google.common.collect.ImmutableSet;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import org.apache.beam.rewriter.common.UsesPackage;
import org.openrewrite.ExecutionContext;
import org.openrewrite.Recipe;
import org.openrewrite.TreeVisitor;
import org.openrewrite.java.ChangeType;
import org.openrewrite.java.JavaIsoVisitor;
import org.openrewrite.java.tree.J;
import org.openrewrite.java.tree.J.MethodDeclaration;
import org.openrewrite.java.tree.JavaType;

/** See {@link #getDescription()}. */
public class FunctionRecipe extends Recipe {

  @Override
  public String getDisplayName() {
    return "Convert Spark Function2 to Beam SerializableBiFunction";
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
    return new UsesPackage<>("org.apache.spark.api.java.function");
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
              "org.apache.spark.api.java.function.Function",
              "org.apache.beam.sdk.transforms.SerializableFunction",
              true));
      doAfterVisit(
          new ChangeType(
              "org.apache.spark.api.java.function.Function2",
              "org.apache.beam.sdk.transforms.SerializableBiFunction",
              true));
      doAfterVisit(
          new ChangeType(
              "org.apache.spark.api.java.function.MapFunction",
              "org.apache.beam.sdk.transforms.SerializableFunction",
              true));
      return c;
    }

    @Override
    public MethodDeclaration visitMethodDeclaration(
        MethodDeclaration method, ExecutionContext executionContext) {
      MethodDeclaration m = super.visitMethodDeclaration(method, executionContext);

      if (m.getSimpleName().equals("call")) {

        // Some functions we just have to change the name + remove throws
        if (m.getMethodType().getDeclaringType().getInterfaces().stream()
            .anyMatch(
                fq ->
                    fq.getFullyQualifiedName()
                            .equals("org.apache.spark.api.java.function.Function2")
                        || fq.getFullyQualifiedName()
                            .equals("org.apache.spark.api.java.function.Function")
                        || fq.getFullyQualifiedName()
                            .equals("org.apache.spark.api.java.function.MapFunction"))) {
          JavaType.Method methodType =
              m.getMethodType().withName("apply").withThrownExceptions(List.of());
          return m.withMethodType(methodType)
              .withName(m.getName().withSimpleName("apply"))
              .withThrows(List.of());
        }
      }
      return m;
    }
  }
}
