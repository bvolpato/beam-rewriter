package org.apache.beam.rewriter;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;
import net.lingala.zip4j.ZipFile;
import org.apache.beam.rewriter.common.CookbookConfig;
import org.apache.beam.rewriter.common.CookbookEnum;
import org.apache.beam.rewriter.common.CookbookFactory;
import org.openrewrite.ExecutionContext;
import org.openrewrite.InMemoryExecutionContext;
import org.openrewrite.Result;
import org.openrewrite.java.tree.J;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestPart;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

@RestController
public class RewriterController {

  @GetMapping("/recipes")
  public List<String> getRecipes() {
    return List.of("Apache Spark to Apache Beam", "Apache Flink to Apache Beam");
  }

  @PostMapping("/convert")
  public String convert(String cookbook, String code) throws IOException {

    CookbookConfig cookbookConfig = CookbookFactory.buildCookbook(CookbookEnum.get(cookbook));
    ExecutionContext ctx = new InMemoryExecutionContext(Throwable::printStackTrace);

    Path tempFolder = Files.createTempDirectory("beam-rewriter");
    Path file = Files.writeString(tempFolder.resolve("Custom.java"), code);

    // parser the source files into LSTs
    List<J.CompilationUnit> cus = cookbookConfig.getParser().parse(List.of(file), tempFolder, ctx);
    List<Result> results = cookbookConfig.getCookbook().run(cus, ctx).getResults();

    if (results.isEmpty()) {
      return code;
    }

    return results.get(0).getAfter().printAll();
  }

  @PostMapping("/convertProject")
  public ResponseEntity<byte[]> convertProject(
      @RequestPart("cookbook") String cookbook,
      @RequestPart("file") MultipartFile file) throws IOException {

    String extension = StringUtils.getFilenameExtension(file.getOriginalFilename());
    CookbookConfig cookbookConfig = CookbookFactory.buildCookbook(CookbookEnum.get(cookbook));

    ExecutionContext ctx = new InMemoryExecutionContext(Throwable::printStackTrace);
    Path tempFolder = Files.createTempDirectory("beam-rewriter");

    if (extension.equals("zip")) {
      Path tempZip = Files.createTempFile("beam-rewriter", ".zip");
      Path workingZipFile = Files.write(tempZip, file.getBytes());

      System.out.println("Extract tempFolder: " + tempFolder);
      new ZipFile(workingZipFile.toFile()).extractAll(tempFolder.toFile().getAbsolutePath());

      List<Path> sourcePaths = Files.find(tempFolder, 999, (p, bfa) ->
              bfa.isRegularFile() && p.getFileName().toString().endsWith(".java"))
          .collect(Collectors.toList());
      List<J.CompilationUnit> cus = cookbookConfig.getParser().parse(sourcePaths, tempFolder, ctx);
      List<Result> results = cookbookConfig.getCookbook().run(cus, ctx).getResults();
      for (Result result : results) {
        System.out.println("Rewriting " + result.getAfter().getSourcePath());

        Files.writeString(tempFolder.resolve(result.getAfter().getSourcePath()),
            result.getAfter().printAll());
      }

      Path tempFolderZip = Files.createTempDirectory("beam-rewriter");
      Path targetZip = tempFolderZip.resolve(file.getOriginalFilename());
      System.out.println("Extract targetZip: " + targetZip);

      ZipFile zipFile = new ZipFile(targetZip.toFile());

      File[] files = tempFolder.toFile().listFiles();
      for (File addFile : files) {
        if (addFile.isDirectory()) {
          zipFile.addFolder(addFile);
        } else {
          zipFile.addFile(addFile);
        }
      }

      return ResponseEntity.ok()
          .contentType(MediaType.APPLICATION_OCTET_STREAM)
          .body(Files.readAllBytes(targetZip));

    } else {
      Path workingFile = Files.write(tempFolder.resolve(file.getOriginalFilename()),
          file.getBytes());

      // parser the source files into LSTs
      List<J.CompilationUnit> cus = cookbookConfig.getParser()
          .parse(List.of(workingFile), tempFolder, ctx);
      List<Result> results = cookbookConfig.getCookbook().run(cus, ctx).getResults();

      return ResponseEntity.ok()
          .contentType(MediaType.APPLICATION_OCTET_STREAM)
          .body(results.get(0).getAfter().printAllAsBytes());

    }


  }
}