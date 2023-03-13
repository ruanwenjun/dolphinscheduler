package org.apache.dolphinscheduler.alert.content.template;

import org.apache.dolphinscheduler.alert.api.content.AlertContent;
import org.apache.dolphinscheduler.alert.api.content.WorkflowTimeCheckNotRunAlertContent;
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
@ConditionalOnProperty(prefix = "alert.template", name = "enable", havingValue = "true")
public class WorkflowTimeCheckNotRunAlertTemplateInjector extends BaseAlertTemplateInjector {

    public WorkflowTimeCheckNotRunAlertTemplateInjector(AlertConfig alertConfig) {
        super(alertConfig);
    }

    @Override
    public @NonNull TemplateInjectedAlertContentWrapper injectIntoTemplate(AlertContent alertContent) {
        WorkflowTimeCheckNotRunAlertContent workflowTimeCheckNotRunAlertContent =
                (WorkflowTimeCheckNotRunAlertContent) alertContent;
        String title = alertTemplate.getTitleTemplate()
                .replaceAll(TemplateInjectUtils.ALERT_TYPE_TEMPLATE,
                        AlertContentUtils.getAlertType(workflowTimeCheckNotRunAlertContent.getAlertType()))
                .replaceAll(TemplateInjectUtils.PROJECT_NAME_TEMPLATE,
                        workflowTimeCheckNotRunAlertContent.getProjectName())
                .replaceAll(TemplateInjectUtils.WORKFLOW_INSTANCE_NAME_TEMPLATE,
                        workflowTimeCheckNotRunAlertContent.getWorkflowInstanceName())
                .replaceAll(TemplateInjectUtils.WORKFLOW_NAME_TEMPLATE,
                        workflowTimeCheckNotRunAlertContent.getWorkflowName())
                .replaceAll(TemplateInjectUtils.ALERT_CREATE_TIME_TEMPLATE,
                        DateUtils.formatDate(workflowTimeCheckNotRunAlertContent.getAlertCreateTime()));

        String content = alertTemplate.getContentTemplate()
                .replaceAll(TemplateInjectUtils.ALERT_TYPE_TEMPLATE,
                        AlertContentUtils.getAlertType(workflowTimeCheckNotRunAlertContent.getAlertType()))
                .replaceAll(TemplateInjectUtils.PROJECT_NAME_TEMPLATE,
                        workflowTimeCheckNotRunAlertContent.getProjectName())
                .replaceAll(TemplateInjectUtils.WORKFLOW_INSTANCE_NAME_TEMPLATE,
                        workflowTimeCheckNotRunAlertContent.getWorkflowInstanceName())
                .replaceAll(TemplateInjectUtils.WORKFLOW_NAME_TEMPLATE,
                        workflowTimeCheckNotRunAlertContent.getWorkflowName())
                .replaceAll(TemplateInjectUtils.ALERT_CREATE_TIME_TEMPLATE,
                        DateUtils.formatDate(workflowTimeCheckNotRunAlertContent.getAlertCreateTime()));

        return TemplateInjectedAlertContentWrapper.builder()
                .alertContentPojo(alertContent)
                .alertTitle(title)
                .alertContent(content)
                .build();
    }

    @Override
    public @NonNull AlertType getAlertType() {
        return AlertType.WORKFLOW_TIME_CHECK_NOT_RUN_ALERT;
    }
}
