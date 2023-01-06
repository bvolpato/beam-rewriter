import java.util.Arrays;
import java.util.UUID;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

public class WordCounterJoined {

  private static final String FILE_NAME_1 = "samples/kinglear.txt";
  private static final String FILE_NAME_2 = "samples/hamlet.txt";

  public static void main(String[] args) {

    SparkConf sparkConf =
        new SparkConf()
            .setMaster("local")
            .setAppName("WordCount Sample")
            .set("spark.driver.bindAddress", "127.0.0.1")
            .set("spark.driver.host", "127.0.0.1");

    JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

    JavaRDD<String> inputFile1 = sparkContext.textFile(FILE_NAME_1);
    JavaRDD<String> inputFile2 = sparkContext.textFile(FILE_NAME_2);
    JavaRDD<String> joined = inputFile1.union(inputFile2);

    JavaRDD<String> wordsFromFile =
        joined
            .flatMap(content -> Arrays.stream(content.split(" ")).iterator())
            .map(word -> word.replaceAll("[^a-zA-Z0-9]", ""))
            .filter(word -> word.length() > 1);

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
