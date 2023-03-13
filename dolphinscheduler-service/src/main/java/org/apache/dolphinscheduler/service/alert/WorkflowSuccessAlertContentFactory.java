package org.apache.dolphinscheduler.service.alert;

import org.apache.dolphinscheduler.alert.api.content.WorkflowSuccessAlertContent;
import org.apache.dolphinscheduler.alert.api.enums.AlertType;
import org.apache.dolphinscheduler.dao.entity.ProcessInstance;
import org.apache.dolphinscheduler.dao.entity.ProjectUser;
import org.apache.dolphinscheduler.dao.entity.TaskInstance;

import java.util.Date;

import com.google.auto.service.AutoService;

import lombok.NonNull;

@AutoService(AlertContentFactory.class)
public class WorkflowSuccessAlertContentFactory implements AlertContentFactory<WorkflowSuccessAlertContent> {

    @Override
    public WorkflowSuccessAlertContent generateAlertContent(@NonNull ProcessInstance processInstance,
                                                            @NonNull ProjectUser projectUser,
                                                            TaskInstance taskInstances) {
        return WorkflowSuccessAlertContent.builder()
                .projectName(projectUser.getProjectName())
                .workflowInstanceName(processInstance.getName())
                .alertCreateTime(new Date())
                .startTime(processInstance.getStartTime())
                .endTime(processInstance.getEndTime())
                .build();
    }

    @Override
    public AlertType getAlertType() {
        return AlertType.PROCESS_INSTANCE_SUCCESS;
    }
}
