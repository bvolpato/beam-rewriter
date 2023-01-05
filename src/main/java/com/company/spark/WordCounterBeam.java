package com.company.spark;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;

public class WordCounterBeam {

  private static final String FILE_NAME = "samples/shakespeare.txt";

  public static void main(String[] args) {

    PipelineOptions pipelineOptions = PipelineOptionsFactory.create();

    Pipeline pipeline = Pipeline.create(pipelineOptions);

    PCollection<String> inputFile = pipeline.apply("ReadTextFile", TextIO.read().from(FILE_NAME));
    // inputFile.apply(Combine.glo)
    PCollection<String> wordsFromFile =
        inputFile
            // .flatMap(content -> Arrays.stream(content.split(" ")).iterator())
            .apply(
                "Map",
                MapElements.into(TypeDescriptors.strings())
                    .via(word -> word.replaceAll("[^a-zA-Z0-9]", "")))
            .apply("Filter", Filter.by(word -> word.length() > 1));

    // PCollection<KV<String, Integer>> countData = wordsFromFile.apply("MapToPair",
    // MapElements.into(TypeDescriptors.kvs(TypeDescriptor.of(String.class),
    // TypeDescriptor.of(Integer.class))).via(t -> KV.of(t, 1)))
    //         .apply("Combine", Combine.perKey((x, y) -> x + y);

    // countData.saveAsTextFile("target/CountData/" + UUID.randomUUID());
  }
}
