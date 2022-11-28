package org.apache.dolphinscheduler.service.alert;

import lombok.NonNull;
import lombok.experimental.UtilityClass;
import org.apache.dolphinscheduler.common.enums.Flag;
import org.apache.dolphinscheduler.common.enums.WarningType;
import org.apache.dolphinscheduler.dao.entity.ProcessInstance;

@UtilityClass
public class AlertUtils {

    public boolean isNeedToSendWorkflowAlert(@NonNull ProcessInstance processInstance) {
        if (Flag.YES == processInstance.getIsSubProcess()) {
            return false;
        }
        boolean sendWarning = false;
        WarningType warningType = processInstance.getWarningType();
        switch (warningType) {
            case ALL:
                if (processInstance.getState().typeIsFinished()) {
                    sendWarning = true;
                }
                break;
            case SUCCESS:
                if (processInstance.getState().typeIsSuccess()) {
                    sendWarning = true;
                }
                break;
            case FAILURE:
                if (processInstance.getState().typeIsFailure()) {
                    sendWarning = true;
                }
                break;
            default:
        }
        return sendWarning;
    }
}
