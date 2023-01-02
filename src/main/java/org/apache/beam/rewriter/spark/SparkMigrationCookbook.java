package org.apache.beam.rewriter.spark;

import org.openrewrite.Recipe;
import org.openrewrite.java.format.AutoFormat;

public class SparkMigrationCookbook extends Recipe {

  @Override
  public String getDisplayName() {
    return "Migrate Spark to Beam";
  }

  @Override
  public String getDescription() {
    return "Migrate Spark to Beam.";
  }

  public SparkMigrationCookbook() {
    doNext(new JavaRDDFilterRecipe());
    doNext(new JavaRDDMapRecipe());
    doNext(new TupleToKVRecipe());
    doNext(new JavaRDDtoPCollectionRecipe());
    doNext(new JavaPairRDDtoPCollectionRecipe());
    doNext(new SparkContextTextFileToBeamTextIORecipe());
    doNext(new SparkContextToPipelineRecipe());
    doNext(new AutoFormat());
  }
}