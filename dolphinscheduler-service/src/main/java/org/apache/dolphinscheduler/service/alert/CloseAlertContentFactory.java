package org.apache.dolphinscheduler.service.alert;

import com.google.auto.service.AutoService;
import lombok.NonNull;
import org.apache.dolphinscheduler.alert.api.content.CloseAlertContent;
import org.apache.dolphinscheduler.alert.api.enums.AlertType;
import org.apache.dolphinscheduler.dao.entity.ProcessInstance;
import org.apache.dolphinscheduler.dao.entity.ProjectUser;
import org.apache.dolphinscheduler.dao.entity.TaskInstance;

@AutoService(AlertContentFactory.class)
public class CloseAlertContentFactory implements AlertContentFactory<CloseAlertContent> {

    @Override
    public CloseAlertContent generateAlertContent(@NonNull ProcessInstance processInstance,
                                                  @NonNull ProjectUser projectUser,
                                                  TaskInstance taskInstances) {

        return CloseAlertContent.builder()
                .projectName(projectUser.getProjectName())
                .workflowInstanceName(processInstance.getName())
                .build();
    }

    @Override
    public AlertType getAlertType() {
        return AlertType.CLOSE_ALERT;
    }
}
