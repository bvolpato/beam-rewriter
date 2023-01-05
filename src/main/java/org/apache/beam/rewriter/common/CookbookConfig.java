package org.apache.beam.rewriter.common;

import org.openrewrite.Recipe;
import org.openrewrite.java.JavaParser;

/** Configuration holder for Beam Rewriter. */
public class CookbookConfig {

  private final String name;
  private final Recipe cookbook;
  private final JavaParser parser;

  public CookbookConfig(String name, Recipe cookbook, JavaParser parser) {
    this.name = name;
    this.cookbook = cookbook;
    this.parser = parser;
  }

  public String getName() {
    return name;
  }

  public Recipe getCookbook() {
    return cookbook;
  }

  public JavaParser getParser() {
    return parser;
  }
}
