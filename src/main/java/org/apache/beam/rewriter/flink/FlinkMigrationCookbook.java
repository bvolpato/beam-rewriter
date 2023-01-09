package org.apache.beam.rewriter.flink;

import org.apache.beam.rewriter.beam.AddMissingPipelineRunRecipe;
import org.apache.beam.rewriter.beam.BeamCleanupCookbook;
import org.openrewrite.Recipe;

/** See {@link #getDescription()}. */
public class FlinkMigrationCookbook extends Recipe {

  public FlinkMigrationCookbook() {
    // https://github.com/apache/flink/tree/master/flink-examples/flink-examples-streaming/src/main/java/org/apache/flink/streaming/examples

    doNext(new DataStreamtoPCollectionRecipe());

    doNext(new AddMissingPipelineRunRecipe());
    doNext(new BeamCleanupCookbook());
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
