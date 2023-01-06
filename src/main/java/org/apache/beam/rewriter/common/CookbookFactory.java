package org.apache.beam.rewriter.common;

import java.util.function.Supplier;
import org.apache.beam.rewriter.flink.FlinkMigrationCookbook;
import org.apache.beam.rewriter.spark.SparkMigrationCookbook;
import org.jetbrains.annotations.NotNull;
import org.openrewrite.java.JavaParser;

/** Factory class for Beam Rewriter instances. */
public final class CookbookFactory {

  /**
   * Create a Beam Rewriter Config for a cookbook.
   *
   * @param cookbook Cookbook type.
   * @return Populated instance to use.
   */
  public static CookbookConfig buildCookbook(CookbookEnum cookbook) {
    switch (cookbook) {
      case SPARK:
        return new CookbookConfig(
            "Beam to Spark", new SparkMigrationCookbook(), buildParser(cookbook).build());
      case FLINK:
        return new CookbookConfig(
            "Beam to Flink", new FlinkMigrationCookbook(), buildParser(cookbook).build());
      default:
        throw new IllegalArgumentException("Invalid cookbook: " + cookbook);
    }
  }

  public static JavaParser.Builder<? extends JavaParser, ?> buildParser(CookbookEnum cookbook) {
    switch (cookbook) {
      case SPARK:
        return JavaParser.fromJavaVersion().classpath("beam-sdks-java-core", "spark", "scala");
      case FLINK:
        return JavaParser.fromJavaVersion()
            .classpath("beam-sdks-java-core", "flink-streaming", "flink-core", "scala");
      default:
        throw new IllegalArgumentException("Invalid cookbook for parser: " + cookbook);
    }
  }

  public static Supplier<JavaParser> beamParser() {
    return () -> beamBuilder().build();
  }

  public static JavaParser.Builder<? extends JavaParser, ?> beamBuilder() {
    return JavaParser.fromJavaVersion().classpath("beam-sdks-java-core");
  }
}
