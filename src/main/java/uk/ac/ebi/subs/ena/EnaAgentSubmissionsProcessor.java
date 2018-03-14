package uk.ac.ebi.subs.ena;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.core.RabbitMessagingTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.converter.MessageConverter;
import org.springframework.stereotype.Service;
import uk.ac.ebi.subs.data.submittable.Submittable;
import uk.ac.ebi.subs.ena.processor.ENAAgentProcessor;
import uk.ac.ebi.subs.ena.processor.ENAProcessorContainerService;
import uk.ac.ebi.subs.messaging.Exchanges;
import uk.ac.ebi.subs.messaging.Queues;
import uk.ac.ebi.subs.messaging.Topics;
import uk.ac.ebi.subs.processing.ProcessingCertificate;
import uk.ac.ebi.subs.processing.ProcessingCertificateEnvelope;
import uk.ac.ebi.subs.processing.SubmissionEnvelope;
import uk.ac.ebi.subs.processing.UpdatedSamplesEnvelope;
import uk.ac.ebi.subs.validator.data.SingleValidationResult;

import java.util.ArrayList;
import java.util.List;


@Service
public class EnaAgentSubmissionsProcessor {

    private static final Logger logger = LoggerFactory.getLogger(EnaAgentSubmissionsProcessor.class);

    RabbitMessagingTemplate rabbitMessagingTemplate;

    ENAProcessorContainerService enaProcessorContainerService;

    FileMoveService fileMoveService;

    @Autowired
    public EnaAgentSubmissionsProcessor(RabbitMessagingTemplate rabbitMessagingTemplate, MessageConverter messageConverter,
                                        ENAProcessorContainerService enaProcessorContainerService,
                                        FileMoveService fileMoveService) {
        this.rabbitMessagingTemplate = rabbitMessagingTemplate;
        this.rabbitMessagingTemplate.setMessageConverter(messageConverter);
        this.enaProcessorContainerService = enaProcessorContainerService;
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

        for (ENAAgentProcessor enaAgentProcessor : enaProcessorContainerService.getENAAgentProcessorList()) {
            final List<Submittable> submittables = enaAgentProcessor.getSubmittables(submissionEnvelope);
            for (Submittable submittable : submittables) {
                ProcessingCertificate processingCertificate = enaAgentProcessor.processAndConvertSubmittable(submittable,new ArrayList<SingleValidationResult>());
                processingCertificateList.add(processingCertificate);
            }
        }

        return new ProcessingCertificateEnvelope(submissionEnvelope.getSubmission().getId(),processingCertificateList);
    }


}