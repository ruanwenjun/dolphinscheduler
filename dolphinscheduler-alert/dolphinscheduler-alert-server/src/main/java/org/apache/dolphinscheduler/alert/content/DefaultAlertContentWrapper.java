package org.apache.dolphinscheduler.alert.content;

import lombok.AllArgsConstructor;
import org.apache.dolphinscheduler.common.utils.JSONUtils;
import org.apache.dolphinscheduler.dao.dto.alert.AlertContent;

@AllArgsConstructor
public class DefaultAlertContentWrapper implements AlertContentWrapper {

    private final AlertContent alertContent;

    @Override
    public String getAlertTitle() {
        return String.format("[WhaleScheduler-%s] %s", alertContent.getAlertType().getDescp(),
                alertContent.getProjectName());
    }

    @Override
    public String getAlertContent() {
        return JSONUtils.writeAsPrettyString(alertContent);
    }
}
