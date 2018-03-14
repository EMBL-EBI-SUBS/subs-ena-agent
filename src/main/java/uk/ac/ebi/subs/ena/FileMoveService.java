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

    @Value("${ena.file_move.clusterName}")
    private String clusterName;

    @Value("${ena.file_move.username}")
    private String username;

    private static final String FILE_SEPARATOR = System.getProperty("file.separator");

    public void moveFile(String sourcePath) {

        String targetPath = String.join(FILE_SEPARATOR,
                webinFolderPath , sourcePath.substring(sourcePath.indexOf(sourceBaseFolder) + sourceBaseFolder.length() + 1)
        );

        String targetFolder = targetPath.substring(0, targetPath.lastIndexOf(FILE_SEPARATOR));

        LOGGER.info("Moving a file from {} to {}.", sourcePath, targetPath);

        ProcessBuilder processBuilder = new ProcessBuilder("ssh",
                clusterName + "@" + username,
                "move_file_to_archive_storage.sh",
                sourcePath,
                targetPath,
                targetFolder);

        int exitValue = 0;
        Process process;

        try {
            process = processBuilder.start();
            process.waitFor();
            exitValue = process.exitValue();
        } catch (Exception e) {
            throw new RuntimeException(
                    String.format("The file move command went wrong with file: %s.", sourcePath));
        }

        if (exitValue != 0) {
            throw new RuntimeException(
                    String.format("The file move command went wrong with file: %s.", sourcePath));
        }
    }
}
