package org.apache.dolphinscheduler.alert.content.template;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.dolphinscheduler.alert.config.AlertConfig;
import org.apache.dolphinscheduler.alert.content.TemplateInjectedAlertContentWrapper;
import org.apache.dolphinscheduler.common.enums.AlertType;
import org.apache.dolphinscheduler.dao.dto.alert.AlertContent;
import org.apache.dolphinscheduler.dao.dto.alert.WorkflowFailureAlertContent;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@ConditionalOnProperty(prefix = "alert.template", name = "enable")
public class WorkflowInstanceFailureAlertContentTemplateInjector extends BaseAlertTemplateInjector {

    public WorkflowInstanceFailureAlertContentTemplateInjector(AlertConfig alertConfig) {
        super(alertConfig);
    }

    @Override
    public @NonNull TemplateInjectedAlertContentWrapper injectIntoTemplate(AlertContent alertContent) {
        WorkflowFailureAlertContent workflowFailureAlertContent = (WorkflowFailureAlertContent) alertContent;
        String title = alertTemplate.getTitleTemplate()
                .replaceAll(TemplateInjectUtils.ALERT_TYPE_TEMPLATE,
                        workflowFailureAlertContent.getAlertType().getDescp())
                .replaceAll(TemplateInjectUtils.PROJECT_NAME_TEMPLATE, workflowFailureAlertContent.getProjectName())
                .replaceAll(TemplateInjectUtils.WORKFLOW_INSTANCE_NAME_TEMPLATE,
                        workflowFailureAlertContent.getWorkflowInstanceName());

        String content = alertTemplate.getContentTemplate()
                .replaceAll(TemplateInjectUtils.ALERT_TYPE_TEMPLATE,
                        workflowFailureAlertContent.getAlertType().getDescp())
                .replaceAll(TemplateInjectUtils.PROJECT_NAME_TEMPLATE, workflowFailureAlertContent.getProjectName())
                .replaceAll(TemplateInjectUtils.WORKFLOW_INSTANCE_NAME_TEMPLATE,
                        workflowFailureAlertContent.getWorkflowInstanceName());

        return TemplateInjectedAlertContentWrapper.builder()
                .alertContentPojo(alertContent)
                .alertTitle(title)
                .alertContent(content)
                .build();
    }

    @Override
    public @NonNull AlertType getAlertType() {
        return AlertType.PROCESS_INSTANCE_FAILURE;
    }
}
