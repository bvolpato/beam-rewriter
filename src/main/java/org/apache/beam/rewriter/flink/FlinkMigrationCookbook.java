package org.apache.beam.rewriter.flink;

import org.apache.beam.rewriter.spark.JavaPairRDDtoPCollectionRecipe;
import org.apache.beam.rewriter.spark.JavaRDDFilterRecipe;
import org.apache.beam.rewriter.spark.JavaRDDMapRecipe;
import org.apache.beam.rewriter.spark.JavaRDDtoPCollectionRecipe;
import org.apache.beam.rewriter.spark.SparkContextTextFileToBeamTextIORecipe;
import org.apache.beam.rewriter.spark.SparkContextToPipelineRecipe;
import org.apache.beam.rewriter.spark.TupleToKVRecipe;
import org.openrewrite.Recipe;
import org.openrewrite.java.format.AutoFormat;

public class FlinkMigrationCookbook extends Recipe {

  @Override
  public String getDisplayName() {
    return "Migrate Flink to Beam";
  }

  @Override
  public String getDescription() {
    return "Migrate Flink to Beam.";
  }

  public FlinkMigrationCookbook() {
    // https://github.com/apache/flink/tree/master/flink-examples/flink-examples-streaming/src/main/java/org/apache/flink/streaming/examples

    doNext(new DataStreamtoPCollectionRecipe());
    doNext(new AutoFormat());
  }
}