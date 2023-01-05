package org.apache.beam.rewriter.spark;

import org.openrewrite.Recipe;
import org.openrewrite.java.format.AutoFormat;

public class SparkMigrationCookbook extends Recipe {

  public SparkMigrationCookbook() {
    doNext(new JavaRDDFilterRecipe());
    doNext(new JavaRDDReduceRecipe());
    doNext(new JavaRDDReduceByKeyRecipe());
    doNext(new JavaRDDMapRecipe());
    doNext(new JavaRDDMapToPairRecipe());
    doNext(new TupleToKVRecipe());
    doNext(new JavaRDDtoPCollectionRecipe());
    doNext(new JavaPairRDDtoPCollectionRecipe());
    doNext(new SparkContextTextFileToBeamTextIORecipe());
    doNext(new SparkConfToPipelineOptionsRecipe());
    doNext(new SparkContextToPipelineRecipe());
    doNext(new AutoFormat());
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
