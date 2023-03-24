package org.apache.dolphinscheduler.service.alert;

import org.apache.dolphinscheduler.alert.api.content.TaskFailureAlertContent;
import org.apache.dolphinscheduler.alert.api.enums.AlertType;
import org.apache.dolphinscheduler.dao.entity.ProcessInstance;
import org.apache.dolphinscheduler.dao.entity.ProjectUser;
import org.apache.dolphinscheduler.dao.entity.TaskInstance;

import java.util.Date;

import com.google.auto.service.AutoService;

import lombok.NonNull;

@AutoService(AlertContentFactory.class)
public class TaskFailureAlertContentFactory implements AlertContentFactory<TaskFailureAlertContent> {

    @Override
    public TaskFailureAlertContent generateAlertContent(@NonNull ProcessInstance processInstance,
                                                        @NonNull ProjectUser projectUser,
                                                        @NonNull TaskInstance taskInstances) {
        return TaskFailureAlertContent.builder()
                .projectName(projectUser.getProjectName())
                .workflowInstanceName(processInstance.getName())
                .taskName(taskInstances.getName())
                .alertCreateTime(new Date())
                .startTime(taskInstances.getStartTime())
                .endTime(taskInstances.getEndTime())
                .build();
    }

    @Override
    public AlertType getAlertType() {
        return AlertType.TASK_FAILURE;
    }
}