package org.apache.dolphinscheduler.alert.content.template;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.dolphinscheduler.alert.config.AlertConfig;
import org.apache.dolphinscheduler.alert.content.TemplateInjectedAlertContentWrapper;
import org.apache.dolphinscheduler.common.enums.AlertType;
import org.apache.dolphinscheduler.dao.dto.alert.AlertContent;
import org.apache.dolphinscheduler.dao.dto.alert.WorkflowTimeCheckNotRunAlertContent;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

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
                        workflowTimeCheckNotRunAlertContent.getAlertType().getDescp())
                .replaceAll(TemplateInjectUtils.PROJECT_NAME_TEMPLATE,
                        workflowTimeCheckNotRunAlertContent.getProjectName())
                .replaceAll(TemplateInjectUtils.WORKFLOW_INSTANCE_NAME_TEMPLATE,
                        workflowTimeCheckNotRunAlertContent.getWorkflowInstanceName())
                .replaceAll(TemplateInjectUtils.WORKFLOW_NAME_TEMPLATE,
                        workflowTimeCheckNotRunAlertContent.getWorkflowName());

        String content = alertTemplate.getContentTemplate()
                .replaceAll(TemplateInjectUtils.ALERT_TYPE_TEMPLATE,
                        workflowTimeCheckNotRunAlertContent.getAlertType().getDescp())
                .replaceAll(TemplateInjectUtils.PROJECT_NAME_TEMPLATE,
                        workflowTimeCheckNotRunAlertContent.getProjectName())
                .replaceAll(TemplateInjectUtils.WORKFLOW_INSTANCE_NAME_TEMPLATE,
                        workflowTimeCheckNotRunAlertContent.getWorkflowInstanceName())
                .replaceAll(TemplateInjectUtils.WORKFLOW_NAME_TEMPLATE,
                        workflowTimeCheckNotRunAlertContent.getWorkflowName());

        return TemplateInjectedAlertContentWrapper.builder()
                .alertTitle(title)
                .alertContent(content)
                .build();
    }

    @Override
    public @NonNull AlertType getAlertType() {
        return AlertType.WORKFLOW_TIME_CHECK_NOT_RUN_ALERT;
    }
}
