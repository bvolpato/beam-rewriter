package org.apache.beam.rewriter.beam;

import org.openrewrite.Recipe;
import org.openrewrite.java.format.AutoFormat;

/** See {@link #getDescription()}. */
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
