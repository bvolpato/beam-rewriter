package org.apache.beam.rewriter.beam;

import com.google.common.collect.ImmutableSet;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.beam.rewriter.common.CookbookFactory;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.openrewrite.ExecutionContext;
import org.openrewrite.Recipe;
import org.openrewrite.TreeVisitor;
import org.openrewrite.java.JavaIsoVisitor;
import org.openrewrite.java.JavaTemplate;
import org.openrewrite.java.MethodMatcher;
import org.openrewrite.java.search.UsesType;
import org.openrewrite.java.tree.J;
import org.openrewrite.java.tree.J.MethodInvocation;
import org.openrewrite.java.tree.JavaType;
import org.openrewrite.java.tree.JavaType.Parameterized;
import org.openrewrite.java.tree.JavaType.ShallowClass;
import org.openrewrite.marker.Markers;

public class TypeDescriptorRecipe extends Recipe {

  @Override
  public String getDisplayName() {
    return "Tide up TypeDescriptor.of() with natural methods";
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
    return new UsesType<>("org.apache.beam.sdk.values.TypeDescriptor");
  }

  @Override
  public JavaIsoVisitor<ExecutionContext> getVisitor() {
    return new Visitor();
  }

  static class Visitor extends JavaIsoVisitor<ExecutionContext> {

    private static final Map<Class<?>, String> classToName = new HashMap<>() {
      {
        // TODO: finish the list
        put(String.class, "strings");
        put(Integer.class, "integers");
        put(Row.class, "rows");
        put(BigInteger.class, "bigintegers");
        put(BigDecimal.class, "bigdecimals");
        put(Double.class, "doubles");
        put(Boolean.class, "booleans");
        put(Byte.class, "bytes");
        put(Long.class, "longs");
        put(Void.class, "voids");
      }
    };
    final MethodMatcher filterMatcher =
        new MethodMatcher("org.apache.beam.sdk.values.TypeDescriptor of(..)", false);

    @Override
    public J.MethodInvocation visitMethodInvocation(
        J.MethodInvocation method, ExecutionContext executionContext) {
      MethodInvocation mi = super.visitMethodInvocation(method, executionContext);
      if (filterMatcher.matches(method)
          && method.getArguments().get(0).getType() instanceof JavaType.Parameterized
          && ((Parameterized) method.getArguments().get(0).getType())
              .getFullyQualifiedName()
              .equals("java.lang.Class")) {
        JavaType.Class javaType =
            (JavaType.Class)
                ((Parameterized) method.getArguments().get(0).getType()).getTypeParameters().get(0);
        String typeQualifiedName = javaType.getFullyQualifiedName();
        String typeShortName = javaType.getClassName();

        Class<?> aClass = null;
        try {
          aClass = Class.forName(typeQualifiedName);
        } catch (ClassNotFoundException e) {
          e.printStackTrace();
        }
        if (aClass != null && classToName.containsKey(aClass)) {
          String simpleName = classToName.get(aClass);
          JavaType.ShallowClass fq =
              ShallowClass.build("org.apache.beam.sdk.values.TypeDescriptors");
          mi =
              mi.withSelect(
                      new J.Identifier(
                          UUID.randomUUID(),
                          mi.getSelect().getPrefix(),
                          Markers.EMPTY,
                          fq.getClassName(),
                          fq,
                          null))
                  .withName(method.getName().withSimpleName(simpleName))
                  .withTemplate(
                      JavaTemplate.builder(this::getCursor, "TypeDescriptors." + simpleName + "()")
                          .imports("org.apache.beam.sdk.values.TypeDescriptors")
                          .javaParser(CookbookFactory.beamParser())
                          .build(),
                      method.getCoordinates().replaceMethod());
          maybeAddImport("org.apache.beam.sdk.values.TypeDescriptors");
          maybeRemoveImport("org.apache.beam.sdk.values.TypeDescriptor");
        }
        return mi;
      } else {
        return mi;
      }
    }
  }
}
