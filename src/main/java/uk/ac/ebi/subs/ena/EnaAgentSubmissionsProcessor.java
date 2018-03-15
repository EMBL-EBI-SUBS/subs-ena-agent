package uk.ac.ebi.subs.ena;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.core.RabbitMessagingTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.converter.MessageConverter;
import org.springframework.stereotype.Service;
import uk.ac.ebi.subs.data.component.Archive;
import uk.ac.ebi.subs.data.status.ProcessingStatusEnum;
import uk.ac.ebi.subs.ena.processor.ENAProcessor;
import uk.ac.ebi.subs.messaging.Exchanges;
import uk.ac.ebi.subs.messaging.Queues;
import uk.ac.ebi.subs.messaging.Topics;
import uk.ac.ebi.subs.processing.ProcessingCertificate;
import uk.ac.ebi.subs.processing.ProcessingCertificateEnvelope;
import uk.ac.ebi.subs.processing.SubmissionEnvelope;
import uk.ac.ebi.subs.processing.UpdatedSamplesEnvelope;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;


@Service
public class EnaAgentSubmissionsProcessor {

    private static final Logger logger = LoggerFactory.getLogger(EnaAgentSubmissionsProcessor.class);

    RabbitMessagingTemplate rabbitMessagingTemplate;

    ENAProcessor enaProcessor;

    FileMoveService fileMoveService;

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

    private void moveUploadedFilesToArchive(SubmissionEnvelope submissionEnvelope) {
        submissionEnvelope.getUploadedFiles().forEach( uploadedFile -> {
            fileMoveService.moveFile(uploadedFile.getPath());
        });
    }

    ProcessingCertificateEnvelope processSubmission(SubmissionEnvelope submissionEnvelope)  {
        List<ProcessingCertificate> processingCertificateList = new ArrayList<>();
        final List<String> errorMessageList = enaProcessor.process(submissionEnvelope);
        if (errorMessageList.isEmpty()) {
            processingCertificateList = submissionEnvelope.allSubmissionItemsStream().map(
                    submittable -> new ProcessingCertificate(
                            submittable, Archive.Ena, ProcessingStatusEnum.Completed, submittable.getAccession())).collect(Collectors.toList());
        } else {
            processingCertificateList = submissionEnvelope.allSubmissionItemsStream().map(
                    submittable -> new ProcessingCertificate(
                            submittable, Archive.Ena, ProcessingStatusEnum.Error)).collect(Collectors.toList());
        }


        return new ProcessingCertificateEnvelope(submissionEnvelope.getSubmission().getId(),processingCertificateList);
    }


}