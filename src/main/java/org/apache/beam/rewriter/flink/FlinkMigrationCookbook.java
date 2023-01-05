package org.apache.beam.rewriter.flink;

import org.openrewrite.Recipe;
import org.openrewrite.java.format.AutoFormat;

public class FlinkMigrationCookbook extends Recipe {

  public FlinkMigrationCookbook() {
    // https://github.com/apache/flink/tree/master/flink-examples/flink-examples-streaming/src/main/java/org/apache/flink/streaming/examples

    doNext(new DataStreamtoPCollectionRecipe());
    doNext(new AutoFormat());
  }

  @Override
  public String getDisplayName() {
    return "Migrate Flink to Beam";
  }

  @Override
  public String getDescription() {
    return "Migrate Flink to Beam.";
  }
}
