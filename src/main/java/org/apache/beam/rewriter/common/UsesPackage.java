/*
 * Copyright 2020 the original author or authors.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * https://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.rewriter.common;

import org.openrewrite.internal.lang.Nullable;
import org.openrewrite.java.JavaIsoVisitor;
import org.openrewrite.java.tree.Flag;
import org.openrewrite.java.tree.J;
import org.openrewrite.java.tree.JavaSourceFile;
import org.openrewrite.java.tree.JavaType;
import org.openrewrite.java.tree.JavaType.FullyQualified;
import org.openrewrite.java.tree.TypeUtils;
import org.openrewrite.marker.SearchResult;

/**
 * Visitor that validates if a class uses a specific package.
 *
 * @param <P> Context type
 */
public class UsesPackage<P> extends JavaIsoVisitor<P> {
  private final String packageName;

  public UsesPackage(String packageName) {
    this.packageName = packageName;
  }

  @Override
  public JavaSourceFile visitJavaSourceFile(JavaSourceFile cu, P p) {
    JavaSourceFile c = cu;

    for (JavaType.Method method : c.getTypesInUse().getUsedMethods()) {
      if (method.hasFlags(Flag.Static)) {
        if ((c = maybeMark(c, method.getDeclaringType())) != cu) {
          return c;
        }
      }
    }

    for (JavaType type : c.getTypesInUse().getTypesInUse()) {
      JavaType checkType =
          type instanceof JavaType.Primitive ? type : TypeUtils.asFullyQualified(type);
      if ((c = maybeMark(c, checkType)) != cu) {
        return c;
      }
    }

    for (J.Import anImport : c.getImports()) {
      if (anImport.isStatic()) {
        if ((c =
                maybeMark(
                    c, TypeUtils.asFullyQualified(anImport.getQualid().getTarget().getType())))
            != cu) {
          return c;
        }
      } else if ((c = maybeMark(c, TypeUtils.asFullyQualified(anImport.getQualid().getType())))
          != cu) {
        return c;
      }
    }

    return c;
  }

  private JavaSourceFile maybeMark(JavaSourceFile c, @Nullable JavaType type) {
    if (type == null) {
      return c;
    }

    if (type instanceof JavaType.FullyQualified
        && ((FullyQualified) type).getFullyQualifiedName().startsWith(packageName)) {
      return SearchResult.found(c);
    }

    return c;
  }
}
