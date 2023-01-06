import java.util.Arrays;
import java.util.Locale;
import java.util.UUID;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

public class WordCounter {

  private static final String FILE_NAME = "samples/kinglear.txt";

  public static void main(String[] args) {

    SparkConf sparkConf =
        new SparkConf()
            .setMaster("local")
            .setAppName("WordCount Sample")
            .set("spark.driver.bindAddress", "127.0.0.1")
            .set("spark.driver.host", "127.0.0.1");

    JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

    JavaRDD<String> inputFile = sparkContext.textFile(FILE_NAME);

    JavaRDD<String> wordsFromFile =
        inputFile
            .flatMap(content -> Arrays.stream(content.split(" ")).iterator())
            .map(word -> word.replaceAll("[^a-zA-Z0-9]", ""))
            .filter(word -> word.length() > 1)
            .filter(word -> word.toLowerCase().contains("king"));

    JavaPairRDD<String, Integer> countData =
        wordsFromFile.mapToPair(t -> new Tuple2<>(t, 1)).reduceByKey(new SumFunction());

    JavaRDD<String> result = countData.map(t2 -> t2._1 + "," + t2._2);
    result.foreach(line -> System.out.println(line));

    result.saveAsTextFile("target/CountData/" + UUID.randomUUID());
  }

  static class SumFunction implements Function2<Integer, Integer, Integer> {
    @Override
    public Integer call(Integer v1, Integer v2) throws Exception {
      return v1 + v2;
    }
  }
}
