import java.util.Arrays;
import java.util.UUID;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SerializableBiFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.spark.api.java.JavaRDD;

public class WordCounterJoinedBeam {

  private static final String FILE_NAME = "gs://dataflow-samples/shakespeare/kinglear.txt";

  public static void main(String[] args) {

    PipelineOptions pipelineOptions = PipelineOptionsFactory.create();

    Pipeline pipeline = Pipeline.create(pipelineOptions);

    PCollection<String> inputFile1 = pipeline.apply("ReadTextFile", TextIO.read().from(FILE_NAME));
    PCollection<String> inputFile2 = pipeline.apply("ReadTextFile", TextIO.read().from(FILE_NAME));
    PCollection<String> joined = PCollectionList.of(inputFile1).and(inputFile2).apply("Flatten",
        Flatten.pCollections());

    PCollection<String> wordsFromFile =
        joined
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
            .apply("CombinePerKey", Combine.perKey(new SumFunction()));

    PCollection<String> result =
        countData.apply(
            "Map",
            MapElements.into(TypeDescriptors.strings())
                .via(t2 -> t2.getKey() + "," + t2.getValue()));
    result.apply(
        "ForEach",
        MapElements.into(TypeDescriptors.voids())
            .via(
                line -> {
                  System.out.println(line);
                  return null;
                }));

    result.apply("WriteTextFile", TextIO.write().to("target/CountData/" + UUID.randomUUID()));
    pipeline.run();
  }

  static class SumFunction implements SerializableBiFunction<Integer, Integer, Integer> {
    @Override
    public Integer apply(Integer v1, Integer v2) {
      return v1 + v2;
    }
  }
}
