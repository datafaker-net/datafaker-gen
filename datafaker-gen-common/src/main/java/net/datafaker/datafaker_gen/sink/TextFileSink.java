package net.datafaker.datafaker_gen.sink;

import net.datafaker.datafaker_gen.util.WriteMode;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.function.Function;

public class TextFileSink implements Sink {
    @Override
    public String getName() {
        return "textfile";
    }

    @Override
    public void run(Map<String, ?> config, Function<Integer, ?> function, int numberOfLines) {
        String filepath = (String) config.get("filepath");
        String mode = (String) config.get("mode");
        boolean createParentDirs = (boolean) config.get("create_parent_dirs");
        Path path = Paths.get(filepath);

        handlePathAndDirsTree(path, createParentDirs);

        switch (WriteMode.getWriteMode(mode)) {
            case OVERWRITE:
                break;
            case ERROR_IF_EXISTS:
                if (Files.exists(path)) {
                    throw new RuntimeException("File '" + filepath + "' already exists. " +
                            "Current write mode is: '" + mode + "'.");
                } else {
                    break;
                }
            case UNDEFINED:
                throw new RuntimeException("Undefined write mode: '" + mode + "'. " +
                        "Possible modes: " + String.join(", ", WriteMode.getLegalValues()));
        }

        try (BufferedWriter bw = Files.newBufferedWriter(path)) {
            int numberOfLinesToPrint = numberOfLines;
            int batchSize = getBatchSize(config);
            while (numberOfLinesToPrint > 0) {
                int numberOfLinesToPrintCurrentIteration = Math.min(batchSize, numberOfLinesToPrint);
                bw.write((String) function.apply(numberOfLinesToPrintCurrentIteration));
                numberOfLinesToPrint -= numberOfLinesToPrintCurrentIteration;
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void handlePathAndDirsTree(Path path, boolean createParentDirs) {
        if (Files.isDirectory(path)) {
            throw new RuntimeException("'" + path + "' is a directory, " +
                    "'filepath' config has to be a path to the file.");
        }

        if (isDirectoryTreeExists(path)) {
            return;
        }

        if (!createParentDirs) {
            throw new RuntimeException("'" + path.getParent() + "' directory doesn't exist. " +
                    "Set 'create_parent_dirs' to 'true' to allow automatic file path creation.");
        } else {
            try {
                Files.createDirectories(path.getParent());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private boolean isDirectoryTreeExists(Path path) {
        if (path.getNameCount() == 1) {
            return true;
        }

        return Files.exists(path.getParent());
    }
}
