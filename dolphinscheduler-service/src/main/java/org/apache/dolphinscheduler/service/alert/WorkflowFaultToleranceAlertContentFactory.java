package org.apache.dolphinscheduler.service.alert;

import com.google.auto.service.AutoService;
import lombok.NonNull;
import org.apache.dolphinscheduler.alert.api.content.WorkflowFaultToleranceAlertContent;
import org.apache.dolphinscheduler.alert.api.enums.AlertType;
import org.apache.dolphinscheduler.dao.entity.ProcessInstance;
import org.apache.dolphinscheduler.dao.entity.ProjectUser;
import org.apache.dolphinscheduler.dao.entity.TaskInstance;

@AutoService(AlertContentFactory.class)
public class WorkflowFaultToleranceAlertContentFactory
        implements
            AlertContentFactory<WorkflowFaultToleranceAlertContent> {

    @Override
    public WorkflowFaultToleranceAlertContent generateAlertContent(@NonNull ProcessInstance processInstance,
                                                                   @NonNull ProjectUser projectUser,
                                                                   TaskInstance taskInstances) {
        return WorkflowFaultToleranceAlertContent.builder()
                .projectName(projectUser.getProjectName())
                .workflowInstanceName(processInstance.getName())
                .build();
    }

    @Override
    public AlertType getAlertType() {
        return AlertType.WORKFLOW_FAULT_TOLERANCE;
    }
}
