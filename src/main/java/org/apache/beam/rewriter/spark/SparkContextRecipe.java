package org.apache.beam.rewriter.spark;

import com.google.common.collect.ImmutableSet;
import java.time.Duration;
import java.util.Set;
import org.apache.beam.rewriter.common.CookbookFactory;
import org.openrewrite.ExecutionContext;
import org.openrewrite.Recipe;
import org.openrewrite.TreeVisitor;
import org.openrewrite.java.ChangeType;
import org.openrewrite.java.JavaTemplate;
import org.openrewrite.java.JavaVisitor;
import org.openrewrite.java.MethodMatcher;
import org.openrewrite.java.search.UsesType;
import org.openrewrite.java.tree.J;
import org.openrewrite.java.tree.JavaType;

public class SparkContextRecipe extends Recipe {

  @Override
  public String getDisplayName() {
    return "Convert SparkContext to Beam Pipeline";
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
  public JavaVisitor<ExecutionContext> getVisitor() {
    return new Visitor();
  }

  static class Visitor extends JavaVisitor<ExecutionContext> {
    MethodMatcher pipelineConstructorMatcher =
        new MethodMatcher("org.apache.spark.api.java.JavaSparkContext <constructor>(..)", true);

    @Override
    public J.CompilationUnit visitCompilationUnit(J.CompilationUnit cu, ExecutionContext ctx) {
      J.CompilationUnit c = (J.CompilationUnit) super.visitCompilationUnit(cu, ctx);
      doAfterVisit(
          new ChangeType(
              "org.apache.spark.api.java.JavaSparkContext", "org.apache.beam.sdk.Pipeline", true));
      return c;
    }

    @Override
    public J.Identifier visitIdentifier(J.Identifier identifier, ExecutionContext ctx) {
      identifier = (J.Identifier) super.visitIdentifier(identifier, ctx);
      if (identifier.getSimpleName().equals("sparkContext")) {
        return identifier.withSimpleName("pipeline");
      }
      return identifier;
    }

    @Override
    public J visitNewClass(J.NewClass newClass, ExecutionContext ctx) {
      System.out.println("visitNewClass: " + newClass);
      if (pipelineConstructorMatcher.matches(newClass)) {
        JavaType.Method ctorType = newClass.getConstructorType();

        return newClass.withTemplate(
            JavaTemplate.builder(this::getCursor, "Pipeline.create(#{any()})")
                .imports("org.apache.beam.sdk.Pipeline")
                .javaParser(CookbookFactory.beamParser())
                .build(),
            newClass.getCoordinates().replace(),
            newClass.getArguments().get(0));
      }

      return super.visitNewClass(newClass, ctx);
    }
  }
}
