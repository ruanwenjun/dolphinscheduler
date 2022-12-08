package org.apache.dolphinscheduler.service.alert;

import lombok.NonNull;
import org.apache.dolphinscheduler.alert.api.content.AlertContent;
import org.apache.dolphinscheduler.alert.api.enums.AlertType;
import org.apache.dolphinscheduler.dao.entity.ProcessInstance;
import org.apache.dolphinscheduler.dao.entity.ProjectUser;
import org.apache.dolphinscheduler.dao.entity.TaskInstance;

public interface AlertContentFactory<C extends AlertContent> {

    @NonNull
    C generateAlertContent(@NonNull ProcessInstance processInstance,
                           @NonNull ProjectUser projectUser,
                           TaskInstance taskInstances);

    @NonNull
    AlertType getAlertType();

}
