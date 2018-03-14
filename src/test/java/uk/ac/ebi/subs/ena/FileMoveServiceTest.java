package uk.ac.ebi.subs.ena;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.util.FileSystemUtils;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertTrue;

@RunWith(SpringRunner.class)
@SpringBootTest
public class FileMoveServiceTest {

    @Autowired
    private FileMoveService fileMoveService;

    @Value("${ena.file_move.webinFolderPath}")
    private String webinFolderPath;

    @Value("${ena.file_move.sourceBaseFolder}")
    private String sourceBaseFolder;

    private static final String FILE_SEPARATOR = System.getProperty("file.separator");

    private String testFileBasePath = "src/test/resources";

    private String fileAdditionalPath = "a/b/abcd1234efgh5678/test_file.cram";

    private String fullSourcePath;

    @Before
    public void setup() throws IOException {
        fullSourcePath = String.join(FILE_SEPARATOR,
                testFileBasePath, sourceBaseFolder, fileAdditionalPath);
        createTestResources(fullSourcePath);
    }

    @After
    public void tearDown() throws IOException {
        FileSystemUtils.deleteRecursively(
                new File(String.join(FILE_SEPARATOR, webinFolderPath)));
        FileSystemUtils.deleteRecursively(
                new File(String.join(FILE_SEPARATOR, testFileBasePath, sourceBaseFolder)));
    }

    @Test
    public void whenMovingExistingFiles_ThenTheyMovedSuccessfully() {
        fileMoveService.moveFile(fullSourcePath);

        assertTrue(Files.exists(Paths.get(String.join(FILE_SEPARATOR, webinFolderPath, fileAdditionalPath))));
    }

    @Test
    public void whenFileExistAlreadyInTargetLocation_ThenFileWillBeStillRemovedFromSourceStorage() throws IOException {
        Path targetPath = Paths.get(String.join(FILE_SEPARATOR, webinFolderPath, fileAdditionalPath));
        Files.createDirectories(targetPath.getParent());
        Files.copy(Paths.get(fullSourcePath), targetPath);
        
        fileMoveService.moveFile(fullSourcePath);

        assertTrue(Files.exists(targetPath));
        assertTrue(!Files.exists(Paths.get(fullSourcePath)));
    }

    private void createTestResources(String fullSourcePath) throws IOException {
        Files.createDirectories(Paths.get(
                String.join(FILE_SEPARATOR,
                        fullSourcePath.substring(0, fullSourcePath.lastIndexOf(FILE_SEPARATOR)))));
        List<String> lines = Arrays.asList("This is a TEST file.", "This is the second line.");
        Path file = Paths.get(fullSourcePath);
        Files.write(file, lines, Charset.forName("UTF-8"));
    }
}
