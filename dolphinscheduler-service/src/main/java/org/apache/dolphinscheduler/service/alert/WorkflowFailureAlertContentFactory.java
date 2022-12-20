package org.apache.dolphinscheduler.service.alert;

import com.google.auto.service.AutoService;
import lombok.NonNull;
import org.apache.dolphinscheduler.alert.api.content.WorkflowFailureAlertContent;
import org.apache.dolphinscheduler.alert.api.enums.AlertType;
import org.apache.dolphinscheduler.dao.entity.ProcessInstance;
import org.apache.dolphinscheduler.dao.entity.ProjectUser;
import org.apache.dolphinscheduler.dao.entity.TaskInstance;

@AutoService(AlertContentFactory.class)
public class WorkflowFailureAlertContentFactory implements AlertContentFactory<WorkflowFailureAlertContent> {

    @Override
    public WorkflowFailureAlertContent generateAlertContent(@NonNull ProcessInstance processInstance,
                                                            @NonNull ProjectUser projectUser,
                                                            TaskInstance taskInstances) {
        return WorkflowFailureAlertContent.builder()
                .projectName(projectUser.getProjectName())
                .workflowInstanceName(processInstance.getName())
                .startTime(processInstance.getStartTime())
                .endTime(processInstance.getEndTime())
                .build();
    }

    @Override
    public AlertType getAlertType() {
        return AlertType.PROCESS_INSTANCE_FAILURE;
    }

}
