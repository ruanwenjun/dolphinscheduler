package org.apache.dolphinscheduler.alert.content.template;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.dolphinscheduler.alert.config.AlertConfig;
import org.apache.dolphinscheduler.alert.content.TemplateInjectedAlertContentWrapper;
import org.apache.dolphinscheduler.common.enums.AlertType;
import org.apache.dolphinscheduler.dao.dto.alert.AlertContent;
import org.apache.dolphinscheduler.dao.dto.alert.WorkflowTimeCheckStillRunningAlertContent;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@ConditionalOnProperty(prefix = "alert.template", name = "enable", havingValue = "true")
public class WorkflowTimeCheckStillRunningAlertTemplateInjector extends BaseAlertTemplateInjector {

    public WorkflowTimeCheckStillRunningAlertTemplateInjector(AlertConfig alertConfig) {
        super(alertConfig);
    }

    @Override
    public @NonNull TemplateInjectedAlertContentWrapper injectIntoTemplate(AlertContent alertContent) {
        WorkflowTimeCheckStillRunningAlertContent workflowTimeCheckStillRunningAlertContent =
                (WorkflowTimeCheckStillRunningAlertContent) alertContent;
        String title = alertTemplate.getTitleTemplate()
                .replaceAll(TemplateInjectUtils.ALERT_TYPE_TEMPLATE,
                        workflowTimeCheckStillRunningAlertContent.getAlertType().getDescp())
                .replaceAll(TemplateInjectUtils.PROJECT_NAME_TEMPLATE,
                        workflowTimeCheckStillRunningAlertContent.getProjectName())
                .replaceAll(TemplateInjectUtils.WORKFLOW_INSTANCE_NAME_TEMPLATE,
                        workflowTimeCheckStillRunningAlertContent.getWorkflowInstanceName())
                .replaceAll(TemplateInjectUtils.WORKFLOW_NAME_TEMPLATE,
                        workflowTimeCheckStillRunningAlertContent.getWorkflowName());

        String content = alertTemplate.getContentTemplate()
                .replaceAll(TemplateInjectUtils.ALERT_TYPE_TEMPLATE,
                        workflowTimeCheckStillRunningAlertContent.getAlertType().getDescp())
                .replaceAll(TemplateInjectUtils.PROJECT_NAME_TEMPLATE,
                        workflowTimeCheckStillRunningAlertContent.getProjectName())
                .replaceAll(TemplateInjectUtils.WORKFLOW_INSTANCE_NAME_TEMPLATE,
                        workflowTimeCheckStillRunningAlertContent.getWorkflowInstanceName())
                .replaceAll(TemplateInjectUtils.WORKFLOW_NAME_TEMPLATE,
                        workflowTimeCheckStillRunningAlertContent.getWorkflowName());

        return TemplateInjectedAlertContentWrapper.builder()
                .alertTitle(title)
                .alertContent(content)
                .build();
    }

    @Override
    public @NonNull AlertType getAlertType() {
        return AlertType.WORKFLOW_TIME_CHECK_STILL_RUNNING_ALERT;
    }
}
