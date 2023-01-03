package org.apache.beam.rewriter;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import net.lingala.zip4j.ZipFile;
import org.apache.beam.rewriter.spark.SparkMigrationCookbook;
import org.openrewrite.ExecutionContext;
import org.openrewrite.InMemoryExecutionContext;
import org.openrewrite.Recipe;
import org.openrewrite.Result;
import org.openrewrite.java.JavaParser;
import org.openrewrite.java.tree.J;
import org.springframework.core.io.InputStreamResource;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestPart;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

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

  @PostMapping("/convertProject")
  public ResponseEntity<InputStreamResource> convertProject(
      @RequestPart("cookbook") String cookbook,
      @RequestPart("file") MultipartFile file) throws IOException {

    String extension = StringUtils.getFilenameExtension(file.getOriginalFilename());

    Recipe recipe = new SparkMigrationCookbook();

    // create a JavaParser instance with your classpath
    JavaParser javaParser = JavaParser.fromJavaVersion()
        .classpath("beam", "spark", "scala")
        .build();

    ExecutionContext ctx = new InMemoryExecutionContext(Throwable::printStackTrace);

    Path tempFolder = Files.createTempDirectory("beam-rewriter");

    if (extension.equals("zip")) {
      Path tempZip = Files.createTempFile("beam-rewriter", ".zip");
      Path workingZipFile = Files.write(tempZip, file.getBytes());

      System.out.println("Extract " + tempFolder);
      new ZipFile(workingZipFile.toFile()).extractAll(tempFolder.toFile().getAbsolutePath());

      List<Path> sourcePaths = Files.find(tempFolder, 999, (p, bfa) ->
              bfa.isRegularFile() && p.getFileName().toString().endsWith(".java"))
          .collect(Collectors.toList());
      List<J.CompilationUnit> cus = javaParser.parse(sourcePaths, tempFolder, ctx);
      List<Result> results = recipe.run(cus, ctx).getResults();
      for (Result result : results) {
        Files.writeString(tempFolder.resolve(result.getAfter().getSourcePath()),
                result.getAfter().printAll());
      }

      Path targetZip = tempFolder.resolve(file.getOriginalFilename());

      new ZipFile(targetZip.toFile()).addFolder(tempFolder.toFile());

      return ResponseEntity.ok()
          .contentType(MediaType.APPLICATION_OCTET_STREAM)
          .body(new InputStreamResource(
              new FileInputStream(targetZip.toFile())));

    } else {
      Path workingFile = Files.write(tempFolder.resolve(file.getOriginalFilename()), file.getBytes());

      // parser the source files into LSTs
      List<J.CompilationUnit> cus = javaParser.parse(List.of(workingFile), tempFolder, ctx);
      List<Result> results = recipe.run(cus, ctx).getResults();

      return ResponseEntity.ok()
          .contentType(MediaType.APPLICATION_OCTET_STREAM)
          .body(new InputStreamResource(
              new ByteArrayInputStream(results.get(0).getAfter().printAllAsBytes())));

    }


  }
}