package org.apache.beam.rewriter.common;

/**
 * Cookbooks allowed on the rewriter.
 */
public enum CookbookEnum {
  SPARK,
  FLINK;

  public static CookbookEnum get(String name) {
    if (name.equalsIgnoreCase("spark")) {
      return SPARK;
    } else if (name.equalsIgnoreCase("flink")) {
      return FLINK;
    }

    throw new IllegalArgumentException("Invalid cookbook name: " + name);
  }
}
