/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import java.util.Arrays;
import java.util.UUID;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

/** Pipeline that reads a text file and counts the frequency of each word. */
public class WordCounter {

  /** File name to read. */
  private static final String FILE_NAME = "samples/kinglear.txt";

  public static void main(String[] args) {

    // Prepare the session
    SparkConf sparkConf =
        new SparkConf()
            .setMaster("local")
            .setAppName("WordCount Sample")
            .set("spark.driver.bindAddress", "127.0.0.1")
            .set("spark.driver.host", "127.0.0.1");
    JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

    // Read the file into lines
    JavaRDD<String> inputFile = sparkContext.textFile(FILE_NAME);

    // Split the lines into words
    JavaRDD<String> wordsFromFile =
        inputFile
            .flatMap(content -> Arrays.stream(content.split(" ")).iterator())
            .map(word -> word.replaceAll("[^a-zA-Z0-9]", ""))
            .filter(word -> word.length() > 1);

    // Count the frequency of each word
    JavaPairRDD<String, Integer> countData =
        wordsFromFile.mapToPair(t -> new Tuple2<>(t, 1)).reduceByKey(new SumFunction());

    // Prepare the output to print
    JavaRDD<String> result = countData.map(t2 -> t2._1 + "," + t2._2);

    // Print to the console
    result.foreach(line -> System.out.println(line));

    // Save the results to a text file
    result.saveAsTextFile("target/CountData/" + UUID.randomUUID());
  }

  /** Function that combines the input using addition. */
  static class SumFunction implements Function2<Integer, Integer, Integer> {
    @Override
    public Integer call(Integer v1, Integer v2) throws Exception {
      return v1 + v2;
    }
  }
}
