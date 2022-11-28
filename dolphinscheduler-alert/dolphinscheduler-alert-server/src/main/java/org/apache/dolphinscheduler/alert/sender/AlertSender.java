package org.apache.dolphinscheduler.alert.sender;

import org.apache.dolphinscheduler.dao.entity.Alert;

import java.util.List;

public interface AlertSender {

    void sendAlert(List<Alert> alertList);
}
