package org.apache.dolphinscheduler.alert.content.template;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.dolphinscheduler.alert.config.AlertConfig;
import org.apache.dolphinscheduler.alert.content.TemplateInjectedAlertContentWrapper;
import org.apache.dolphinscheduler.common.enums.AlertType;
import org.apache.dolphinscheduler.common.utils.JSONUtils;
import org.apache.dolphinscheduler.dao.dto.alert.AlertContent;
import org.apache.dolphinscheduler.dao.dto.alert.TaskResultAlertContent;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@ConditionalOnProperty(prefix = "alert.template", name = "enable", havingValue = "true")
public class TaskResultTemplateAlertContentInjector extends BaseAlertTemplateInjector {

    public TaskResultTemplateAlertContentInjector(AlertConfig alertConfig) {
        super(alertConfig);
    }

    @Override
    public @NonNull TemplateInjectedAlertContentWrapper injectIntoTemplate(AlertContent alertContent) {
        TaskResultAlertContent taskResultAlertContent = (TaskResultAlertContent) alertContent;
        String title = alertTemplate.getTitleTemplate()
                .replaceAll(TemplateInjectUtils.ALERT_TYPE_TEMPLATE, taskResultAlertContent.getAlertType().getDescp())
                .replaceAll(TemplateInjectUtils.TITLE_TEMPLATE, taskResultAlertContent.getTitle())
                .replaceAll(TemplateInjectUtils.TASK_NAME_TEMPLATE, taskResultAlertContent.getTaskName())
                .replaceAll(TemplateInjectUtils.PROJECT_NAME_TEMPLATE, taskResultAlertContent.getProjectName())
                .replaceAll(TemplateInjectUtils.WORKFLOW_INSTANCE_NAME_TEMPLATE,
                        taskResultAlertContent.getWorkflowInstanceName());
        String content = alertTemplate.getContentTemplate()
                .replaceAll(TemplateInjectUtils.ALERT_TYPE_TEMPLATE, taskResultAlertContent.getAlertType().getDescp())
                .replaceAll(TemplateInjectUtils.TITLE_TEMPLATE, taskResultAlertContent.getTitle())
                .replaceAll(TemplateInjectUtils.TASK_NAME_TEMPLATE, taskResultAlertContent.getTaskName())
                .replaceAll(TemplateInjectUtils.PROJECT_NAME_TEMPLATE, taskResultAlertContent.getProjectName())
                .replaceAll(TemplateInjectUtils.WORKFLOW_INSTANCE_NAME_TEMPLATE,
                        taskResultAlertContent.getWorkflowInstanceName())
                .replaceAll(TemplateInjectUtils.RESULT_TEMPLATE,
                        JSONUtils.writeAsPrettyString(taskResultAlertContent.getResult()));

        return TemplateInjectedAlertContentWrapper.builder()
                .alertContentPojo(alertContent)
                .alertTitle(title)
                .alertContent(content)
                .build();
    }

    @Override
    public @NonNull AlertType getAlertType() {
        return AlertType.TASK_RESULT;
    }
}
