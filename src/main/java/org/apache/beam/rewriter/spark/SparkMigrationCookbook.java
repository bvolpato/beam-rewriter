package org.apache.beam.rewriter.spark;

import org.apache.beam.rewriter.beam.AddMissingPipelineRunRecipe;
import org.apache.beam.rewriter.beam.BeamCleanupCookbook;
import org.openrewrite.Recipe;

/**
 * See {@link #getDescription()}.
 */
public class SparkMigrationCookbook extends Recipe {

  public SparkMigrationCookbook() {
    doNext(new JavaRDDFilterRecipe());
    doNext(new JavaRDDForEachRecipe());
    doNext(new JavaRDDReduceRecipe());
    doNext(new JavaRDDReduceByKeyRecipe());
    doNext(new JavaRDDMapRecipe());
    doNext(new JavaRDDMapToPairRecipe());
    doNext(new JavaRDDFlatMapRecipe());
    doNext(new JavaRDDSaveAsTextFileRecipe());
    doNext(new Tuple2Recipe());
    doNext(new JavaRDDRecipe());
    doNext(new JavaPairRDDRecipe());
    doNext(new FunctionRecipe());
    doNext(new SparkContextTextFileRecipe());
    doNext(new SparkConfRecipe());
    doNext(new SparkContextRecipe());

    doNext(new AddMissingPipelineRunRecipe());
    doNext(new BeamCleanupCookbook());
  }

  @Override
  public String getDisplayName() {
    return "Migrate Spark to Beam";
  }

  @Override
  public String getDescription() {
    return "Migrate Spark to Beam.";
  }
}
