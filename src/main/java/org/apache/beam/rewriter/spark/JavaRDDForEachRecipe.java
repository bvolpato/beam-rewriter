package org.apache.beam.rewriter.spark;

import com.google.common.collect.ImmutableSet;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import org.apache.beam.rewriter.common.CookbookFactory;
import org.apache.beam.rewriter.common.UsesPackage;
import org.openrewrite.ExecutionContext;
import org.openrewrite.Recipe;
import org.openrewrite.Tree;
import org.openrewrite.TreeVisitor;
import org.openrewrite.java.JavaIsoVisitor;
import org.openrewrite.java.JavaTemplate;
import org.openrewrite.java.JavaVisitor;
import org.openrewrite.java.MethodMatcher;
import org.openrewrite.java.search.UsesType;
import org.openrewrite.java.tree.J;
import org.openrewrite.java.tree.J.Block;
import org.openrewrite.java.tree.J.Lambda;
import org.openrewrite.java.tree.J.MethodInvocation;
import org.openrewrite.java.tree.JRightPadded;
import org.openrewrite.java.tree.JavaType;
import org.openrewrite.java.tree.Space;
import org.openrewrite.java.tree.Statement;
import org.openrewrite.marker.Markers;

public class JavaRDDForEachRecipe extends Recipe {

  // TODO: hope there would be a better function, going to propose one
  @Override
  public String getDisplayName() {
    return "Replaces JavaRDD ForEach with a MapElements transform";
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
        new MethodMatcher("org.apache.spark.api.java.AbstractJavaRDDLike foreach(..)", false);

    @Override
    public J.MethodInvocation visitMethodInvocation(
        J.MethodInvocation methodZ, ExecutionContext executionContext) {

      J.MethodInvocation mi = super.visitMethodInvocation(methodZ, executionContext);

      if (filterMatcher.matches(mi)) {
        System.out.println("Method1: " + mi.getMethodType());

        mi =
            mi.withName(mi.getName().withSimpleName("apply"))
                .withTemplate(
                    JavaTemplate.builder(
                            this::getCursor,
                            "#{any(PCollection)}.apply(\"ForEach\", MapElements.into(TypeDescriptors.voids()).via(#{any()}))")
                        .imports("org.apache.beam.sdk.transforms.MapElements")
                        .imports("org.apache.beam.sdk.transforms.SerializableFunction")
                        .imports("org.apache.beam.sdk.values.TypeDescriptors")
                        .javaParser(CookbookFactory.beamParser())
                        .build(),
                    mi.getCoordinates().replaceMethod(),
                    mi.getSelect(),
                    new LambdaVisitor().visit(mi.getArguments().get(0), executionContext));
        maybeAddImport("org.apache.beam.sdk.transforms.MapElements");
        maybeAddImport("org.apache.beam.sdk.values.TypeDescriptor");
        maybeAddImport("org.apache.beam.sdk.values.TypeDescriptors");
        return mi;
      } else {
        return mi;
      }
    }
  }

  static class LambdaVisitor extends JavaVisitor<ExecutionContext> {

    @Override
    public J visitLambda(Lambda lambda, ExecutionContext executionContext) {
      lambda = (J.Lambda) super.visitLambda(lambda, executionContext);

      if (lambda.getBody() instanceof J.MethodInvocation) {
        return lambda
            .withBody(
                methodInvocationIntoBlock((MethodInvocation) lambda.getBody(), executionContext))
            .withType(
                JavaType.ShallowClass.build("org.apache.beam.sdk.transforms.SerializableFunction")
                    .withTypeParameters(
                        List.of(
                            ((JavaType.FullyQualified) lambda.getType()).getTypeParameters().get(0),
                            JavaType.buildType("java.lang.Void"))));
      }
      return lambda;
    }

    public J.Block methodInvocationIntoBlock(
        J.MethodInvocation mi, ExecutionContext executionContext) {
      mi = (J.MethodInvocation) super.visitMethodInvocation(mi, executionContext);

      System.out.println(mi.getType() + " - " + mi);

      J.Return returnStatement =
          (J.Return)
              (((J.MethodDeclaration)
                      CookbookFactory.beamParser()
                          .get()
                          .parse(
                              "public class Wrapper { public Object run() { return null; } }")
                          .get(0)
                          .getClasses()
                          .get(0)
                          .getBody()
                          .getStatements()
                          .get(0))
                  .getBody()
                  .getStatements()
                  .get(0));

      // JRightPadded buildBlock = JRightPadded.build(buildBlock(mi).withPrefix(Space.EMPTY));
      Block block =
          buildBlock(
              List.of(
                  JRightPadded.build(mi.withPrefix(Space.EMPTY)),
                  JRightPadded.build(returnStatement.withPrefix(Space.EMPTY))));
      return block;
    }

    /**
     * A {@link J.Block} implies the section of code is implicitly surrounded in braces. We can use
     * that to our advantage by saying if you aren't a block (e.g. a single {@link Statement},
     * etc.), then we're going to make this into a block. That's how we'll get the code bodies
     * surrounded in braces.
     */
    private static J.Block buildBlock(List<JRightPadded<Statement>> statements) {
      return new J.Block(
          Tree.randomId(),
          Space.EMPTY,
          Markers.EMPTY,
          JRightPadded.build(false),
          statements,
          Space.EMPTY);
    }
  }
}
