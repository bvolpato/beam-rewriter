package org.apache.beam.rewriter;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.apache.beam.rewriter.spark.SparkMigrationCookbook;
import org.openrewrite.ExecutionContext;
import org.openrewrite.InMemoryExecutionContext;
import org.openrewrite.Recipe;
import org.openrewrite.Result;
import org.openrewrite.java.JavaParser;
import org.openrewrite.java.tree.J;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class ConvertController {

  @GetMapping("/recipes")
  public List<String> getRecipes() {
    return List.of("Apache Spark to Apache Beam", "Apache Flink to Apache Beam");
  }

  @PostMapping("/convert")
  public String convert(String cookbook, String code) throws IOException {

    Recipe recipe = new SparkMigrationCookbook();

    // create a JavaParser instance with your classpath
    JavaParser javaParser = JavaParser.fromJavaVersion()
        .classpath("beam", "spark", "scala")
        .build();

    ExecutionContext ctx = new InMemoryExecutionContext(Throwable::printStackTrace);

    Path tempFolder = Files.createTempDirectory("beam-rewriter");
    Path file = Files.writeString(tempFolder.resolve("Custom.java"), code);

    // parser the source files into LSTs
    List<J.CompilationUnit> cus = javaParser.parse(List.of(file), tempFolder, ctx);
    List<Result> results = recipe.run(cus, ctx).getResults();

    if (results.isEmpty()) {
      return code;
    }

    return results.get(0).getAfter().printAll();
  }
}