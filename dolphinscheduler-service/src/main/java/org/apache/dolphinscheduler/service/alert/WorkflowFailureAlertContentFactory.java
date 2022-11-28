package org.apache.dolphinscheduler.service.alert;

import com.google.auto.service.AutoService;
import lombok.NonNull;
import org.apache.dolphinscheduler.common.enums.AlertType;
import org.apache.dolphinscheduler.dao.dto.alert.WorkflowFailureAlertContent;
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
                .build();
    }

    @Override
    public AlertType getAlertType() {
        return AlertType.PROCESS_INSTANCE_FAILURE;
    }

}
