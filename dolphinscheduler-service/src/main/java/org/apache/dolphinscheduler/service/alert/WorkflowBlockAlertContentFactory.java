package org.apache.dolphinscheduler.service.alert;

import com.google.auto.service.AutoService;
import lombok.NonNull;
import org.apache.dolphinscheduler.alert.api.content.WorkflowBlockAlertContent;
import org.apache.dolphinscheduler.alert.api.enums.AlertType;
import org.apache.dolphinscheduler.dao.entity.ProcessInstance;
import org.apache.dolphinscheduler.dao.entity.ProjectUser;
import org.apache.dolphinscheduler.dao.entity.TaskInstance;

import java.util.Date;

@AutoService(AlertContentFactory.class)
public class WorkflowBlockAlertContentFactory implements AlertContentFactory<WorkflowBlockAlertContent> {

    @Override
    public WorkflowBlockAlertContent generateAlertContent(@NonNull ProcessInstance processInstance,
                                                          @NonNull ProjectUser projectUser,
                                                          TaskInstance taskInstances) {
        return WorkflowBlockAlertContent.builder()
                .projectName(projectUser.getProjectName())
                .workflowInstanceName(processInstance.getName())
                .alertCreateTime(new Date())
                .build();
    }

    @Override
    public AlertType getAlertType() {
        return AlertType.PROCESS_INSTANCE_BLOCKED;
    }
}
