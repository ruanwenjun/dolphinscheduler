package org.apache.dolphinscheduler.alert.content.template;

import org.apache.dolphinscheduler.alert.api.content.AlertContent;
import org.apache.dolphinscheduler.alert.api.content.WorkflowSuccessAlertContent;
import org.apache.dolphinscheduler.alert.api.enums.AlertType;
import org.apache.dolphinscheduler.alert.config.AlertConfig;
import org.apache.dolphinscheduler.alert.content.TemplateInjectedAlertContentWrapper;
import org.apache.dolphinscheduler.alert.utils.AlertContentUtils;
import org.apache.dolphinscheduler.spi.utils.DateUtils;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
@ConditionalOnProperty(prefix = "alert.template", name = "enable")
public class WorkflowInstanceSuccessAlertContentTemplateInjector extends BaseAlertTemplateInjector {

    public WorkflowInstanceSuccessAlertContentTemplateInjector(AlertConfig alertConfig) {
        super(alertConfig);
    }

    @Override
    public @NonNull TemplateInjectedAlertContentWrapper injectIntoTemplate(AlertContent alertContent) {
        WorkflowSuccessAlertContent workflowSuccessAlertContent = (WorkflowSuccessAlertContent) alertContent;
        String title = alertTemplate.getTitleTemplate()
                .replaceAll(TemplateInjectUtils.ALERT_TYPE_TEMPLATE,
                        AlertContentUtils.getAlertType(workflowSuccessAlertContent.getAlertType()))
                .replaceAll(TemplateInjectUtils.PROJECT_NAME_TEMPLATE, workflowSuccessAlertContent.getProjectName())
                .replaceAll(TemplateInjectUtils.WORKFLOW_INSTANCE_NAME_TEMPLATE,
                        workflowSuccessAlertContent.getWorkflowInstanceName())
                .replaceAll(TemplateInjectUtils.ALERT_CREATE_TIME_TEMPLATE,
                        DateUtils.formatDate(workflowSuccessAlertContent.getAlertCreateTime()))
                .replaceAll(TemplateInjectUtils.WORKFLOW_LABEL, workflowSuccessAlertContent.getLabel())
                .replaceAll(TemplateInjectUtils.WORKFLOW_INSTANCE_LINK,
                        workflowSuccessAlertContent.getWorkflowInstanceLink());

        String content = alertTemplate.getContentTemplate()
                .replaceAll(TemplateInjectUtils.ALERT_TYPE_TEMPLATE,
                        AlertContentUtils.getAlertType(workflowSuccessAlertContent.getAlertType()))
                .replaceAll(TemplateInjectUtils.PROJECT_NAME_TEMPLATE, workflowSuccessAlertContent.getProjectName())
                .replaceAll(TemplateInjectUtils.WORKFLOW_INSTANCE_NAME_TEMPLATE,
                        workflowSuccessAlertContent.getWorkflowInstanceName())
                .replaceAll(TemplateInjectUtils.ALERT_CREATE_TIME_TEMPLATE,
                        DateUtils.formatDate(workflowSuccessAlertContent.getAlertCreateTime()))
                .replaceAll(TemplateInjectUtils.WORKFLOW_LABEL, workflowSuccessAlertContent.getLabel())
                .replaceAll(TemplateInjectUtils.WORKFLOW_INSTANCE_LINK,
                        workflowSuccessAlertContent.getWorkflowInstanceLink());

        return TemplateInjectedAlertContentWrapper.builder()
                .alertContentPojo(alertContent)
                .alertTitle(title)
                .alertContent(content)
                .build();
    }

    @Override
    public @NonNull AlertType getAlertType() {
        return AlertType.PROCESS_INSTANCE_SUCCESS;
    }
}