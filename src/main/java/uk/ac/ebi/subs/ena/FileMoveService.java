package uk.ac.ebi.subs.ena;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

@Service
public class FileMoveService {

    private static final Logger LOGGER = LoggerFactory.getLogger(FileMoveService.class);

    @Value("${ena.file_move.webinFolderPath}")
    private String webinFolderPath;

    @Value("${ena.file_move.sourceBaseFolder}")
    private String sourceBaseFolder;

    private static final String FILE_SEPARATOR = System.getProperty("file.separator");

    public void moveFile(String sourcePath) {

        String targetPath = String.join(FILE_SEPARATOR,
                webinFolderPath , sourcePath.substring(sourcePath.indexOf(sourceBaseFolder) + sourceBaseFolder.length() + 1)
        );

        LOGGER.info("Moving a file from {} to {}.", sourcePath, targetPath);

        try {
            Files.createDirectories(Paths.get(targetPath.substring(0, targetPath.lastIndexOf(FILE_SEPARATOR))));
            Files.move(Paths.get(sourcePath), Paths.get(targetPath));
        } catch (IOException e) {
            throw new RuntimeException("Could not execute the file move command.");
        }
    }
}
