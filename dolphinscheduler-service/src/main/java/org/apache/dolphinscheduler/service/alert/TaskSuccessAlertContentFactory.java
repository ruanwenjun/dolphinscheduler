package org.apache.dolphinscheduler.service.alert;

import com.google.auto.service.AutoService;
import lombok.NonNull;
import org.apache.dolphinscheduler.alert.api.content.TaskSuccessAlert;
import org.apache.dolphinscheduler.alert.api.enums.AlertType;
import org.apache.dolphinscheduler.dao.entity.ProcessInstance;
import org.apache.dolphinscheduler.dao.entity.ProjectUser;
import org.apache.dolphinscheduler.dao.entity.TaskInstance;

import java.util.Date;

@AutoService(AlertContentFactory.class)
public class TaskSuccessAlertContentFactory implements AlertContentFactory<TaskSuccessAlert> {

    @Override
    public TaskSuccessAlert generateAlertContent(@NonNull ProcessInstance processInstance,
                                                 @NonNull ProjectUser projectUser,
                                                 @NonNull TaskInstance taskInstances) {
        return TaskSuccessAlert.builder()
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
        return AlertType.TASK_SUCCESS;
    }
}
