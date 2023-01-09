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
import org.openrewrite.java.tree.J.FieldAccess;
import org.openrewrite.java.tree.JavaType;

/** See {@link #getDescription()}. */
public class Tuple2Recipe extends Recipe {

  @Override
  public String getDisplayName() {
    return "Changes usage of Tuple2 to Beam KV";
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
    return new UsesType<>("scala.Tuple2");
  }

  @Override
  public JavaVisitor<ExecutionContext> getVisitor() {
    return new Visitor();
  }

  static class Visitor extends JavaVisitor<ExecutionContext> {

    MethodMatcher filterMatcher = new MethodMatcher("scala.Tuple2 <constructor>(..)", true);

    @Override
    public J visitMethodInvocation(J.MethodInvocation method, ExecutionContext executionContext) {
      System.out.println("visitMethodInvocation: " + method);
      if (filterMatcher.matches(method)) {
        maybeAddImport("org.apache.beam.sdk.values.KV");
        // return method;
      }

      return super.visitMethodInvocation(method, executionContext);
    }

    // https://github.com/openrewrite/rewrite-migrate-java/blob/main/src/main/java/org/openrewrite/java/migrate/util/UseMapOf.java

    @Override
    public J visitNewClass(J.NewClass newClass, ExecutionContext ctx) {
      System.out.println("visitNewClass: " + newClass);
      if (filterMatcher.matches(newClass)) {
        JavaType.Method ctorType = newClass.getConstructorType();

        return newClass.withTemplate(
            JavaTemplate.builder(this::getCursor, "KV.of(#{any()}, #{any()})")
                .imports("org.apache.beam.sdk.values.KV")
                .javaParser(CookbookFactory.beamParser())
                .build(),
            newClass.getCoordinates().replace(),
            newClass.getArguments().get(0),
            newClass.getArguments().get(1));
      }

      return super.visitNewClass(newClass, ctx);
    }

    @Override
    public J visitCompilationUnit(J.CompilationUnit cu, ExecutionContext ctx) {
      J c = super.visitCompilationUnit(cu, ctx);
      doAfterVisit(new ChangeType("scala.Tuple2", "org.apache.beam.sdk.values.KV", false));
      return c;
    }

    @Override
    public J visitFieldAccess(FieldAccess fa, ExecutionContext executionContext) {
      fa = (FieldAccess) super.visitFieldAccess(fa, executionContext);

      if (fa.getSimpleName().equals("_1")) {
        return fa.withTemplate(
            JavaTemplate.builder(this::getCursor, "#{any(org.apache.beam.sdk.values.KV)}.getKey()")
                .imports("org.apache.beam.sdk.values.KV")
                .javaParser(CookbookFactory.beamParser())
                .build(),
            fa.getCoordinates().replace(),
            fa.getTarget());
      } else if (fa.getSimpleName().equals("_2")) {
        return fa.withTemplate(
            JavaTemplate.builder(
                    this::getCursor, "#{any(org.apache.beam.sdk.values.KV)}.getValue()")
                .imports("org.apache.beam.sdk.values.KV")
                .javaParser(CookbookFactory.beamParser())
                .build(),
            fa.getCoordinates().replace(),
            fa.getTarget());
      }

      return fa;
    }
  }
}
