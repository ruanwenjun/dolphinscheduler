package org.apache.dolphinscheduler.server.master.processor.alert;

import io.netty.channel.Channel;
import lombok.extern.slf4j.Slf4j;
import org.apache.dolphinscheduler.common.utils.JSONUtils;
import org.apache.dolphinscheduler.remote.command.Command;
import org.apache.dolphinscheduler.remote.command.CommandType;
import org.apache.dolphinscheduler.remote.command.alert.TaskAlertRequestCommand;
import org.apache.dolphinscheduler.remote.processor.NettyRequestProcessor;
import org.apache.dolphinscheduler.service.alert.AlertManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class TaskResultAlertRequestProcessor implements NettyRequestProcessor {

    @Autowired
    private AlertManager alertManager;

    @Override
    public void process(Channel channel, Command command) {
        if (CommandType.TASK_RESULT_ALERT_SEND_REQUEST != command.getType()) {
            log.error("The command type is invalided, will discard this command: {}", command);
            return;
        }
        TaskAlertRequestCommand taskAlertRequest =
                JSONUtils.parseObject(command.getBody(), TaskAlertRequestCommand.class);
        if (taskAlertRequest == null) {
            log.error("The command body is null, command: {}", command);
            return;
        }

        alertManager.sendTaskResultAlert(taskAlertRequest);
    }
}
