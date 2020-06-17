package uk.ac.ebi.subs.ena;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import uk.ac.ebi.subs.data.component.Archive;
import uk.ac.ebi.subs.data.component.Team;
import uk.ac.ebi.subs.data.status.ProcessingStatusEnum;
import uk.ac.ebi.subs.data.submittable.Assay;
import uk.ac.ebi.subs.data.submittable.AssayData;
import uk.ac.ebi.subs.data.submittable.Sample;
import uk.ac.ebi.subs.data.submittable.Study;
import uk.ac.ebi.subs.ena.helper.TestHelper;
import uk.ac.ebi.subs.processing.ProcessingCertificate;
import uk.ac.ebi.subs.processing.ProcessingCertificateEnvelope;
import uk.ac.ebi.subs.processing.SubmissionEnvelope;
import uk.ac.ebi.subs.processing.fileupload.UploadedFile;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertThat;

/**
 * Created by neilg on 25/05/2017.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = {EnaAgentApplication.class})
@Category(SSHDependentTest.class)
public class EnaAgentAssayDataSubmissionsProcessorTest {

    @Value("${ena.hostname}")
    String enaHostname;

    @Value("${ena.test_login}")
    String test_login;

    @Value("${ena.file_move.webinFolderPath}")
    String webinFolderPath;

    @Value("${spring.profiles.active:dev}")
    private String activeProfile;

    private String REMOTE_MKDIR_FOR_WEBIN_TEST;
    private String REMOTE_DELETE_DIR_FROM_WEBIN_TEST;

    private static final String ENA_AGENT_SUBMISSIONS_PROCESSOR_TEST_PATH = "EnaAgentSubmissionsProcessorTest";

    private static final String ILLUMINA_GENOME_ANALYZER_INSTRUMENT_MODEL = "Illumina Genome Analyzer";

    private static final Logger LOGGER = LoggerFactory.getLogger(EnaAgentAssayDataSubmissionsProcessorTest.class);

    @Autowired
    EnaAgentSubmissionsProcessor enaAgentSubmissionsProcessor;

    @Autowired
    FileMoveService fileMoveService;

    @Before
    public void setup() {
        REMOTE_MKDIR_FOR_WEBIN_TEST =
                "cd "  + webinFolderPath + " && mkdir -p "
                    + String.join("/", activeProfile, ENA_AGENT_SUBMISSIONS_PROCESSOR_TEST_PATH, "to/the/file");
        REMOTE_DELETE_DIR_FROM_WEBIN_TEST =
                "cd "  + webinFolderPath + " && rm -rf " + String.join("/", activeProfile, ENA_AGENT_SUBMISSIONS_PROCESSOR_TEST_PATH);
    }

    @Test
    @Category(SSHDependentTest.class)
    public void whenUploadedFilesHasData_ThenItWillBeInsertedIntoFileMetadata() throws Exception {
        String alias = UUID.randomUUID().toString();
        final Team team = TestHelper.getTeam("test-team");

        final Study study = TestHelper.getStudy(alias, team,"study_abstract","Whole Genome Sequencing");
        study.setId(UUID.randomUUID().toString());
        uk.ac.ebi.subs.data.Submission submission = new uk.ac.ebi.subs.data.Submission();
        submission.setTeam(team);
        submission.setSubmissionDate(new Date());
        SubmissionEnvelope submissionEnvelope = new SubmissionEnvelope(submission);
        submissionEnvelope.getStudies().add(study);

        final Sample sample = TestHelper.getSample(alias, team);
        submissionEnvelope.getSamples().add(sample);

        final Assay assay = TestHelper.getAssay(
                alias, team, TestAccessions.BIOSAMPLE_ACCESSION, alias, ILLUMINA_GENOME_ANALYZER_INSTRUMENT_MODEL);
        submissionEnvelope.getAssays().add(assay);

        final String filename = UUID.randomUUID().toString() + "_test.fastq.gz";

        final AssayData assayData = TestHelper.getAssayData(alias, team, alias);

        assayData.getFiles().get(0).setName(filename);

        submissionEnvelope.getAssayData().add(assayData);

        UploadedFile uploadedFile = new UploadedFile();
        uploadedFile.setChecksum("1234567890abcdefabcd1234567890ab");
        final String remoteFilePath = "ready_to_agent/" + ENA_AGENT_SUBMISSIONS_PROCESSOR_TEST_PATH + "/to/the/file";

        uploadedFile.setPath(String.join("/", remoteFilePath, filename));
        uploadedFile.setFilename(filename);
        createTestFile(filename);

        executeRemoteCommand("ssh", remoteLogin().toString(), REMOTE_MKDIR_FOR_WEBIN_TEST);
        executeRemoteCommand("scp", filename, getRemoteServerPath(fileMoveService.getRelativeFilePath(remoteFilePath)));

        submissionEnvelope.getUploadedFiles().add(uploadedFile);

        final ProcessingCertificateEnvelope processingCertificateEnvelope = enaAgentSubmissionsProcessor.processSubmission(submissionEnvelope);
        ProcessingCertificate studyProcessingCertificate = new ProcessingCertificate(study, Archive.Ena, ProcessingStatusEnum.Completed, study.getAccession());
        ProcessingCertificate assayProcessingCertificate = new ProcessingCertificate(assay, Archive.Ena, ProcessingStatusEnum.Completed, assay.getAccession());
        ProcessingCertificate assayDataProcessingCertificate = new ProcessingCertificate(assayData, Archive.Ena, ProcessingStatusEnum.Completed, assayData.getAccession());
        assertThat("correct assayData certs",
                processingCertificateEnvelope.getProcessingCertificates(),
                containsInAnyOrder(
                        studyProcessingCertificate,
                        assayProcessingCertificate, assayDataProcessingCertificate
                )
        );

        final List<String> filePaths =
                submissionEnvelope.getUploadedFiles().stream().map(uploadedFileFromSubmissionEnvelope ->
                            String.join("/", activeProfile, fileMoveService.getRelativeFilePath(uploadedFileFromSubmissionEnvelope.getPath()))
                        )
                        .collect(Collectors.toList());

        submissionEnvelope.getAssayData().forEach(processedAssayData -> {
            processedAssayData.getFiles().forEach( file -> {
                assertThat(file.getChecksum(), is(notNullValue()));
                assertThat(filePaths.contains(file.getName()), is(Boolean.TRUE));
            });
        });

        //cleanup
        Files.deleteIfExists(Paths.get(filename));
        executeRemoteCommand("ssh", remoteLogin().toString(), REMOTE_DELETE_DIR_FROM_WEBIN_TEST);
    }

    private void createTestFile(String fastaFileName) throws IOException {
        List<String> lines = Arrays.asList("@SEQ_ID",
                "GATTTGGGGTTCAAAGCAGTATCGATCAAATAGTAAATCCATTTGTTCAACTCACAGTTT",
                "+",
                "!''*((((***+))%%%++)(%%%%).1***-+*''))**55CCF>>>>>>CCCCCCC65");
        Path file = Paths.get(fastaFileName);
        Files.write(file, lines, Charset.forName("UTF-8"));
    }

    private int executeRemoteCommand(String... commandAndparameters) {
        ProcessBuilder processBuilder = new ProcessBuilder(commandAndparameters);

        int exitValue;
        Process process;
        final List<String> command = processBuilder.command();

        try {
            LOGGER.info("The following command will be executed in the remote server: {}", command);

            process = processBuilder.start();
            process.waitFor();
            exitValue = process.exitValue();
        } catch (Exception e) {
            throw new RuntimeException(
                    String.format("Could not execute the following command on the remote server: %s. The root cause: %s",
                            String.join(" ", command), e.getMessage()));
        }

        return exitValue;
    }

    private String getRemoteServerPath(String remotePath) {
        StringBuilder sb = remoteLogin();
        sb.append(":");
        sb.append(String.join("/", webinFolderPath, activeProfile, remotePath));

        return sb.toString();
    }

    private StringBuilder remoteLogin() {
        StringBuilder sb = new StringBuilder();
        sb.append(test_login);
        sb.append("@");
        sb.append(enaHostname);

        return sb;
    }
}