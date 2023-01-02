package org.apache.beam.rewriter.spark;

import com.google.common.collect.ImmutableSet;
import java.time.Duration;
import java.util.Set;
import org.openrewrite.ExecutionContext;
import org.openrewrite.Recipe;
import org.openrewrite.TreeVisitor;
import org.openrewrite.java.ChangeType;
import org.openrewrite.java.JavaIsoVisitor;
import org.openrewrite.java.search.UsesType;
import org.openrewrite.java.tree.Expression;
import org.openrewrite.java.tree.J;

public class JavaPairRDDtoPCollectionRecipe extends Recipe {

  @Override
  public String getDisplayName() {
    return "Convert Spark JavaPairRDD to Beam PCollection";
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
  public JavaIsoVisitor<ExecutionContext> getVisitor() {
    return new Visitor();
  }

  static class Visitor extends JavaIsoVisitor<ExecutionContext> {


    @Override
    public J.CompilationUnit visitCompilationUnit(J.CompilationUnit cu, ExecutionContext ctx) {
      J.CompilationUnit c = super.visitCompilationUnit(cu, ctx);

      doAfterVisit(new ChangeType("org.apache.spark.api.java.JavaPairRDD",
          "org.apache.beam.sdk.values.PCollection", true));
      return c;
    }

    @Override
    public J.VariableDeclarations.NamedVariable visitVariable(J.VariableDeclarations.NamedVariable variable, ExecutionContext ctx) {
      System.out.println("visitVariable " + variable);
      return super.visitVariable(variable, ctx);
    }

    @Override
    public Expression visitExpression(Expression expression, ExecutionContext ctx) {
      System.out.println("visitExpression " + expression.getType() + " - " + expression);
      return super.visitExpression(expression, ctx);
    }

    @Override
    public J.ParameterizedType visitParameterizedType(J.ParameterizedType type, ExecutionContext ctx) {
      System.out.println("visitParameterizedType " + type.getType() + " - " + type);
      return super.visitParameterizedType(type, ctx);
    }

  }

}