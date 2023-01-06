package org.apache.beam.rewriter.spark;

import com.google.common.collect.ImmutableSet;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.openrewrite.ExecutionContext;
import org.openrewrite.Recipe;
import org.openrewrite.Tree;
import org.openrewrite.TreeVisitor;
import org.openrewrite.internal.lang.Nullable;
import org.openrewrite.java.ChangeType;
import org.openrewrite.java.JavaVisitor;
import org.openrewrite.java.TypeMatcher;
import org.openrewrite.java.search.UsesType;
import org.openrewrite.java.tree.Expression;
import org.openrewrite.java.tree.J;
import org.openrewrite.java.tree.J.ParameterizedType;
import org.openrewrite.java.tree.J.Wildcard.Bound;
import org.openrewrite.java.tree.JContainer;
import org.openrewrite.java.tree.JLeftPadded;
import org.openrewrite.java.tree.JRightPadded;
import org.openrewrite.java.tree.JavaType;
import org.openrewrite.java.tree.JavaType.ShallowClass;
import org.openrewrite.java.tree.NameTree;
import org.openrewrite.java.tree.Space;
import org.openrewrite.java.tree.TypeTree;
import org.openrewrite.marker.Markers;

public class JavaPairRDDRecipe extends Recipe {

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
  public JavaVisitor<ExecutionContext> getVisitor() {
    return new Visitor();
  }

  static class Visitor extends JavaVisitor<ExecutionContext> {

    final TypeMatcher typeMatcher = new TypeMatcher("org.apache.spark.api.java.JavaPairRDD", true);

    @Override
    public J visitCompilationUnit(J.CompilationUnit cu, ExecutionContext ctx) {
      J c = super.visitCompilationUnit(cu, ctx);

      doAfterVisit(
          new ChangeType(
              "org.apache.spark.api.java.JavaPairRDD",
              "org.apache.beam.sdk.values.PCollection",
              true));
      return c;
    }

    @Override
    public J visitParameterizedType(J.ParameterizedType t2, ExecutionContext ctx) {
      J.ParameterizedType t = (ParameterizedType) super.visitParameterizedType(t2, ctx);
      if (typeMatcher.matches(t)) {
        JavaType.Class pCollectionTyped =
            JavaType.ShallowClass.build("org.apache.beam.sdk.values.PCollection")
                .withTypeParameters(
                    List.of(
                        ShallowClass.build("org.apache.beam.sdk.values.KV")
                            .withTypeParameters(
                                List.of(
                                    t.getTypeParameters().get(0).getType(),
                                    t.getTypeParameters().get(1).getType()))));

        maybeAddImport("org.apache.beam.sdk.values.KV");
        maybeAddImport("org.apache.beam.sdk.values.PCollection");
        return buildTypeTree(pCollectionTyped, Space.EMPTY);
      }
      return t;
    }

    @Nullable
    private TypeTree buildTypeTree(@Nullable JavaType type, Space space) {
      if (type == null || type instanceof JavaType.Unknown) {
        return null;
      } else if (type instanceof JavaType.Primitive) {
        return new J.Primitive(Tree.randomId(), space, Markers.EMPTY, (JavaType.Primitive) type);
      } else if (type instanceof JavaType.FullyQualified) {

        JavaType.FullyQualified fq = (JavaType.FullyQualified) type;

        J.Identifier identifier =
            new J.Identifier(Tree.randomId(), space, Markers.EMPTY, fq.getClassName(), type, null);

        if (!fq.getTypeParameters().isEmpty()) {
          JContainer<Expression> typeParameters = buildTypeParameters(fq.getTypeParameters());
          if (typeParameters == null) {
            // If there is a problem resolving one of the type parameters, then do not return a type
            // expression for the fully-qualified type.
            return null;
          }
          return new J.ParameterizedType(
              Tree.randomId(), space, Markers.EMPTY, identifier, typeParameters);

        } else {
          maybeAddImport(fq);
          return identifier;
        }
      } else if (type instanceof JavaType.Array) {
        return (buildTypeTree(((JavaType.Array) type).getElemType(), space));
      } else if (type instanceof JavaType.Variable) {
        return buildTypeTree(((JavaType.Variable) type).getType(), space);
      } else if (type instanceof JavaType.GenericTypeVariable) {
        JavaType.GenericTypeVariable genericType = (JavaType.GenericTypeVariable) type;

        if (!genericType.getName().equals("?")) {
          return new J.Identifier(
              Tree.randomId(), space, Markers.EMPTY, genericType.getName(), type, null);
        }
        JLeftPadded<Bound> bound = null;
        NameTree boundedType = null;
        if (genericType.getVariance() == JavaType.GenericTypeVariable.Variance.COVARIANT) {
          bound = new JLeftPadded<>(Space.format(" "), J.Wildcard.Bound.Extends, Markers.EMPTY);
        } else if (genericType.getVariance()
            == JavaType.GenericTypeVariable.Variance.CONTRAVARIANT) {
          bound = new JLeftPadded<>(Space.format(" "), J.Wildcard.Bound.Super, Markers.EMPTY);
        }

        if (!genericType.getBounds().isEmpty()) {
          boundedType = buildTypeTree(genericType.getBounds().get(0), Space.format(" "));
          if (boundedType == null) {
            return null;
          }
        }

        return new J.Wildcard(Tree.randomId(), space, Markers.EMPTY, bound, boundedType);
      }
      return null;
    }

    @Nullable
    private JContainer<Expression> buildTypeParameters(List<JavaType> typeParameters) {
      List<JRightPadded<Expression>> typeExpressions = new ArrayList<>();

      for (JavaType type : typeParameters) {
        Expression typeParameterExpression = (Expression) buildTypeTree(type, Space.EMPTY);
        if (typeParameterExpression == null) {
          return null;
        }
        typeExpressions.add(
            new JRightPadded<>(typeParameterExpression, Space.EMPTY, Markers.EMPTY));
      }
      return JContainer.build(Space.EMPTY, typeExpressions, Markers.EMPTY);
    }
  }
}
