package org.apache.dolphinscheduler.service.alert;

import lombok.NonNull;
import org.apache.dolphinscheduler.common.enums.AlertType;
import org.apache.dolphinscheduler.dao.dto.alert.AlertContent;
import org.apache.dolphinscheduler.dao.entity.ProcessInstance;
import org.apache.dolphinscheduler.dao.entity.ProjectUser;
import org.apache.dolphinscheduler.dao.entity.TaskInstance;

import java.util.List;

public interface AlertContentFactory<C extends AlertContent> {

    @NonNull
    C generateAlertContent(@NonNull ProcessInstance processInstance,
                           @NonNull ProjectUser projectUser,
                           TaskInstance taskInstances);

    @NonNull
    AlertType getAlertType();

}
