package org.apache.dolphinscheduler.service.alert;

import com.google.auto.service.AutoService;
import lombok.NonNull;
import org.apache.dolphinscheduler.common.enums.AlertType;
import org.apache.dolphinscheduler.dao.dto.alert.TaskInstanceTimeoutAlertContent;
import org.apache.dolphinscheduler.dao.entity.ProcessInstance;
import org.apache.dolphinscheduler.dao.entity.ProjectUser;
import org.apache.dolphinscheduler.dao.entity.TaskInstance;

@AutoService(AlertContentFactory.class)
public class TaskTimeoutAlertContentFactory implements AlertContentFactory<TaskInstanceTimeoutAlertContent> {

    @Override
    public TaskInstanceTimeoutAlertContent generateAlertContent(@NonNull ProcessInstance processInstance,
                                                                @NonNull ProjectUser projectUser,
                                                                @NonNull TaskInstance taskInstances) {
        return TaskInstanceTimeoutAlertContent.builder()
                .projectName(projectUser.getProjectName())
                .workflowInstanceName(processInstance.getName())
                .taskName(taskInstances.getName())
                .build();
    }

    @Override
    public AlertType getAlertType() {
        return AlertType.TASK_TIMEOUT;
    }
}
