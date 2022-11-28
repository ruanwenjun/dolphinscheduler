package org.apache.dolphinscheduler.service.alert;

import com.google.auto.service.AutoService;
import lombok.NonNull;
import org.apache.dolphinscheduler.common.enums.AlertType;
import org.apache.dolphinscheduler.dao.dto.alert.TaskFailureAlertContent;
import org.apache.dolphinscheduler.dao.entity.ProcessInstance;
import org.apache.dolphinscheduler.dao.entity.ProjectUser;
import org.apache.dolphinscheduler.dao.entity.TaskInstance;

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
                .build();
    }

    @Override
    public AlertType getAlertType() {
        return AlertType.TASK_FAILURE;
    }
}
