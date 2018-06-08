package uk.ac.ebi.subs.ena;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
public class FileMoveService {

    private static final Logger LOGGER = LoggerFactory.getLogger(FileMoveService.class);

    @Value("${ena.file_move.webinFolderPath}")
    private String webinFolderPath;

    @Value("${ena.file_move.sourceBaseFolder}")
    private String sourceBaseFolder;

    @Value("${ena.file_move.remoteHostName}")
    private String remoteHostName;

    @Value("${ena.file_move.scriptPath}")
    private String scriptPath;

    private static final String FILE_SEPARATOR = System.getProperty("file.separator");

    public void moveFile(String sourcePath) {

        final String relativeFilePath = sourcePath.substring(sourcePath.indexOf(sourceBaseFolder) + sourceBaseFolder.length() + 1);
        final String sourceBasePath = sourcePath.substring(0, sourcePath.indexOf(sourceBaseFolder) + sourceBaseFolder.length());

        LOGGER.info("Moving a file from {} to {}.", sourcePath, String.join(FILE_SEPARATOR, webinFolderPath, relativeFilePath));

        ProcessBuilder processBuilder = new ProcessBuilder("ssh",
                remoteHostName,
                String.join(FILE_SEPARATOR, scriptPath, "move_file_to_archive_storage.sh"),
                relativeFilePath,
                sourceBasePath,
                webinFolderPath);

        int exitValue;
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
