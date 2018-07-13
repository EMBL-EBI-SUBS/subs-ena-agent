package uk.ac.ebi.subs.ena;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.core.RabbitMessagingTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.messaging.converter.MessageConverter;
import org.springframework.stereotype.Service;
import uk.ac.ebi.subs.data.component.Archive;
import uk.ac.ebi.subs.data.component.File;
import uk.ac.ebi.subs.data.status.ProcessingStatusEnum;
import uk.ac.ebi.subs.data.submittable.Project;
import uk.ac.ebi.subs.data.submittable.Sample;
import uk.ac.ebi.subs.data.submittable.Submittable;
import uk.ac.ebi.subs.ena.processor.ENAProcessor;
import uk.ac.ebi.subs.messaging.Exchanges;
import uk.ac.ebi.subs.messaging.Queues;
import uk.ac.ebi.subs.messaging.Topics;
import uk.ac.ebi.subs.processing.ProcessingCertificate;
import uk.ac.ebi.subs.processing.ProcessingCertificateEnvelope;
import uk.ac.ebi.subs.processing.SubmissionEnvelope;
import uk.ac.ebi.subs.processing.UpdatedSamplesEnvelope;
import uk.ac.ebi.subs.processing.fileupload.UploadedFile;
import uk.ac.ebi.subs.validator.data.SingleValidationResult;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

@Service
public class EnaAgentSubmissionsProcessor {

    private static final Logger logger = LoggerFactory.getLogger(EnaAgentSubmissionsProcessor.class);

    @Value("${ena.file_move.webinFolderPath}")
    private String webinFolderPath;

    @Value("${spring.profiles.active:dev}")
    private String activeProfile;

    RabbitMessagingTemplate rabbitMessagingTemplate;

    ENAProcessor enaProcessor;

    FileMoveService fileMoveService;

    private static final ProcessingStatusEnum COMPLETED = ProcessingStatusEnum.Completed;
    private static final ProcessingStatusEnum ERROR = ProcessingStatusEnum.Error;

    @Autowired
    public EnaAgentSubmissionsProcessor(RabbitMessagingTemplate rabbitMessagingTemplate, MessageConverter messageConverter,
                                        ENAProcessor enaProcessor,
                                        FileMoveService fileMoveService) {
        this.rabbitMessagingTemplate = rabbitMessagingTemplate;
        this.rabbitMessagingTemplate.setMessageConverter(messageConverter);
        this.enaProcessor = enaProcessor;
        this.fileMoveService = fileMoveService;
    }

    @RabbitListener(queues = Queues.ENA_SAMPLES_UPDATED)
    public void handleSampleUpdate(UpdatedSamplesEnvelope updatedSamplesEnvelope) {

        logger.info("received updated samples for submission {}", updatedSamplesEnvelope.getSubmissionId());

        updatedSamplesEnvelope.getUpdatedSamples().forEach(s -> {

            logger.info("NOT IMPLEMENTED, updates sample {} using submission {}", s.getAccession(), updatedSamplesEnvelope.getSubmissionId());
        });

        logger.info("finished updating samples for submission {}", updatedSamplesEnvelope.getSubmissionId());
    }


    @RabbitListener(queues = {Queues.ENA_AGENT})
    public void handleSubmission(SubmissionEnvelope submissionEnvelope) {
        moveUploadedFilesToArchive(submissionEnvelope);


        ProcessingCertificateEnvelope processingCertificateEnvelope = processSubmission(submissionEnvelope);

        logger.info("received submission {}, most recent handler was ",
                submissionEnvelope.getSubmission().getId());
        logger.info("processed submission {}", submissionEnvelope.getSubmission().getId());
        rabbitMessagingTemplate.convertAndSend(Exchanges.SUBMISSIONS, Topics.EVENT_SUBMISSION_AGENT_RESULTS, processingCertificateEnvelope);
        logger.info("sent submission {}", submissionEnvelope.getSubmission().getId());

    }

    ProcessingCertificateEnvelope processSubmission(SubmissionEnvelope submissionEnvelope) {
        injectPathAndChecksum(submissionEnvelope);

        final List<SingleValidationResult> validationResultList = enaProcessor.process(submissionEnvelope);

        List<ProcessingCertificate> processingCertificateList = new ArrayList<>();

        ProcessingStatusEnum outcome = (validationResultList.isEmpty()) ? COMPLETED : ERROR;

        Map<String, String> errorLookup = new HashMap<>();

        for (SingleValidationResult vr : validationResultList) {
            errorLookup.put(
                    vr.getEntityUuid(),
                    vr.getMessage()
            );
        }

        if (!validationResultList.isEmpty()) {
            logger.error("error messages during submission: {}", validationResultList);
        }

        for (Submittable submittable : submissionEnvelope.allSubmissionItems()) {
            if (Sample.class.isAssignableFrom(submittable.getClass()) || Project.class.isAssignableFrom(submittable.getClass())) {
                continue; //these objects aren't owned by ENA
            }

            ProcessingCertificate cert = new ProcessingCertificate(
                    submittable,
                    Archive.Ena,
                    outcome
            );
            if (errorLookup.containsKey(submittable.getId())) {
                cert.setMessage(errorLookup.get(submittable.getId()));
            }
            if (submittable.isAccessioned()) {
                cert.setAccession(submittable.getAccession());
            }
            processingCertificateList.add(cert);
        }


        return new ProcessingCertificateEnvelope(submissionEnvelope.getSubmission().getId(), processingCertificateList);
    }

    private void injectPathAndChecksum(SubmissionEnvelope submissionEnvelope) {
        Map<String, UploadedFile> uploadedFileMap = filesByFilename(submissionEnvelope.getUploadedFiles());

        Stream<File> assayDataFileStream = submissionEnvelope.getAssayData().stream().flatMap(ad -> ad.getFiles().stream());
        Stream<File> analysisFileStream = submissionEnvelope.getAnalyses().stream().flatMap(a -> a.getFiles().stream());

        Stream.concat(assayDataFileStream, analysisFileStream).forEach(file -> {
                UploadedFile uploadedFile = uploadedFileMap.get(file.getName());
                file.setChecksum(uploadedFile.getChecksum());
                file.setName(String.join("/", activeProfile, fileMoveService.getRelativeFilePath(uploadedFile.getPath())));
        });

    }

    private void moveUploadedFilesToArchive(SubmissionEnvelope submissionEnvelope) {
        submissionEnvelope.getUploadedFiles().forEach(uploadedFile -> {
            fileMoveService.moveFile(uploadedFile.getPath());
        });
    }

    Map<String, UploadedFile> filesByFilename(List<UploadedFile> files) {
        Map<String, UploadedFile> filesByFilename = new HashMap<>();
        files.forEach(file -> filesByFilename.put(file.getFilename(), file));

        return filesByFilename;
    }
}