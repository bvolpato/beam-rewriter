package org.apache.beam.rewriter.beam;

import org.apache.beam.rewriter.spark.JavaPairRDDRecipe;
import org.apache.beam.rewriter.spark.JavaRDDFilterRecipe;
import org.apache.beam.rewriter.spark.JavaRDDMapRecipe;
import org.apache.beam.rewriter.spark.JavaRDDMapToPairRecipe;
import org.apache.beam.rewriter.spark.JavaRDDRecipe;
import org.apache.beam.rewriter.spark.JavaRDDReduceByKeyRecipe;
import org.apache.beam.rewriter.spark.JavaRDDReduceRecipe;
import org.apache.beam.rewriter.spark.SparkConfRecipe;
import org.apache.beam.rewriter.spark.SparkContextRecipe;
import org.apache.beam.rewriter.spark.SparkContextTextFileRecipe;
import org.apache.beam.rewriter.spark.Tuple2Recipe;
import org.openrewrite.Recipe;
import org.openrewrite.java.format.AutoFormat;

public class BeamCleanupCookbook extends Recipe {

  public BeamCleanupCookbook() {
    doNext(new TypeDescriptorRecipe());
    doNext(new AutoFormat());
  }

  @Override
  public String getDisplayName() {
    return "Improve and Format Beam Code";
  }

  @Override
  public String getDescription() {
    return "Improve and Format Beam Code.";
  }
}
