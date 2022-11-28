package org.apache.dolphinscheduler.service.alert;

import com.google.auto.service.AutoService;
import lombok.NonNull;
import org.apache.dolphinscheduler.common.enums.AlertType;
import org.apache.dolphinscheduler.dao.dto.alert.TaskFaultToleranceAlertContent;
import org.apache.dolphinscheduler.dao.entity.ProcessInstance;
import org.apache.dolphinscheduler.dao.entity.ProjectUser;
import org.apache.dolphinscheduler.dao.entity.TaskInstance;

import java.util.List;
import java.util.stream.Collectors;

@AutoService(AlertContentFactory.class)
public class TaskFaultToleranceAlertContentFactory implements AlertContentFactory<TaskFaultToleranceAlertContent> {

    @Override
    public TaskFaultToleranceAlertContent generateAlertContent(@NonNull ProcessInstance processInstance,
                                                               @NonNull ProjectUser projectUser,
                                                               @NonNull TaskInstance taskInstances) {
        return TaskFaultToleranceAlertContent.builder()
                .projectName(projectUser.getProjectName())
                .workflowInstanceName(processInstance.getName())
                .taskName(taskInstances.getName())
                .build();
    }

    @Override
    public AlertType getAlertType() {
        return AlertType.TASK_FAULT_TOLERANCE;
    }
}
