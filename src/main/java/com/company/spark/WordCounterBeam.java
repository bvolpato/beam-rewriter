package com.company.spark;

import java.util.Arrays;
import java.util.UUID;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WordCounterBeam {

  private static final Logger LOG = LoggerFactory.getLogger(WordCounterBeam.class);

  private static final String FILE_NAME = "samples/shakespeare.txt";

  public static void main(String[] args) {

    LOG.info("Building pipeline...");

    PipelineOptions pipelineOptions = PipelineOptionsFactory.create();

    Pipeline pipeline = Pipeline.create(pipelineOptions);

    PCollection<String> inputFile = pipeline.apply("ReadTextFile", TextIO.read().from(FILE_NAME));

    PCollection<String> wordsFromFile =
        inputFile
            .apply(
                "FlatMap",
                FlatMapElements.into(TypeDescriptors.strings())
                    .via(content -> Arrays.asList(content.split(" "))))
            .apply(
                "Map",
                MapElements.into(TypeDescriptors.strings())
                    .via(word -> word.replaceAll("[^a-zA-Z0-9]", "")))
            .apply("Filter", Filter.by(word -> word.length() > 1));

    PCollection<KV<String, Integer>> countData =
        wordsFromFile
            .apply(
                "MapToPair",
                MapElements.into(
                        TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.integers()))
                    .via(t -> KV.of(t, 1)))
            .apply("CombinePerKey", Combine.perKey((x, y) -> x + y));

    countData
        .apply(
            "Map",
            MapElements.into(TypeDescriptors.strings())
                .via(t2 -> t2.getKey() + "," + t2.getValue()))
        .apply("WriteTextFile", TextIO.write().to("target/CountData/" + UUID.randomUUID()));
    pipeline.run();
  }
}
