package org.apache.dolphinscheduler.alert.content;

import lombok.NonNull;
import org.apache.dolphinscheduler.dao.entity.Alert;

public interface AlertContentWrapperGenerator {

    AlertContentWrapper generateAlertContent(@NonNull Alert alert);
}
