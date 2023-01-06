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
import org.openrewrite.java.search.UsesType;
import org.openrewrite.java.tree.J;
import org.openrewrite.java.tree.J.VariableDeclarations.NamedVariable;
import org.openrewrite.java.tree.JavaType;

public class SparkConfRecipe extends Recipe {

  @Override
  public String getDisplayName() {
    return "Convert SparkConf to Beam PipelineOptions";
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
    return new UsesType<>("org.apache.spark.SparkConf");
  }

  @Override
  public JavaVisitor<ExecutionContext> getVisitor() {
    return new Visitor();
  }

  static class Visitor extends JavaVisitor<ExecutionContext> {

    @Override
    public J visitCompilationUnit(J.CompilationUnit cu, ExecutionContext ctx) {
      J c = super.visitCompilationUnit(cu, ctx);
      doAfterVisit(
          new ChangeType(
              "org.apache.spark.SparkConf", "org.apache.beam.sdk.options.PipelineOptions", true));
      return c;
    }

    @Override
    public J visitVariable(NamedVariable v, ExecutionContext executionContext) {
      v = (NamedVariable) super.visitVariable(v, executionContext);
      System.out.println("visitVariable: " + v);

      if (v.getType() instanceof JavaType.Class
          && ((JavaType.Class) v.getType())
              .getFullyQualifiedName()
              .equals("org.apache.spark.SparkConf")) {
        v =
            v.withInitializer(
                v.getInitializer()
                    .withTemplate(
                        JavaTemplate.builder(this::getCursor, "PipelineOptionsFactory.create()")
                            .imports("org.apache.beam.sdk.options.PipelineOptionsFactory")
                            .javaParser(CookbookFactory.beamParser())
                            .build(),
                        v.getInitializer().getCoordinates().replace()));

        maybeAddImport("org.apache.beam.sdk.options.PipelineOptionsFactory");
        return v;
      }

      return super.visitVariable(v, executionContext);
    }

    @Override
    public J visitIdentifier(J.Identifier identifier, ExecutionContext ctx) {
      identifier = (J.Identifier) super.visitIdentifier(identifier, ctx);
      if (identifier.getSimpleName().equals("sparkConf")) {
        return identifier.withSimpleName("pipelineOptions");
      }
      return identifier;
    }
  }
}
