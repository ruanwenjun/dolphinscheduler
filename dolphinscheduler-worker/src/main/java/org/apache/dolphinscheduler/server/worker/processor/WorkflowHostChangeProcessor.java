package org.apache.dolphinscheduler.server.worker.processor;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;

import org.apache.dolphinscheduler.common.utils.JSONUtils;
import org.apache.dolphinscheduler.common.utils.LoggerUtils;
import org.apache.dolphinscheduler.plugin.task.api.TaskExecutionContext;
import org.apache.dolphinscheduler.plugin.task.api.TaskExecutionContextCacheManager;
import org.apache.dolphinscheduler.remote.command.Command;
import org.apache.dolphinscheduler.remote.command.CommandType;
import org.apache.dolphinscheduler.remote.command.task.WorkflowHostChangeRequest;
import org.apache.dolphinscheduler.remote.command.task.WorkflowHostChangeResponse;
import org.apache.dolphinscheduler.remote.processor.NettyRequestProcessor;
import org.apache.dolphinscheduler.server.worker.message.MessageRetryRunner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.google.common.base.Preconditions;

/**
 * update process host
 * this used when master failover
 */
@Component
public class WorkflowHostChangeProcessor implements NettyRequestProcessor {

    private final Logger logger = LoggerFactory.getLogger(WorkflowHostChangeProcessor.class);

    @Autowired
    private MessageRetryRunner messageRetryRunner;

    @Override
    public void process(Channel channel, Command command) {
        Preconditions.checkArgument(CommandType.WORKFLOW_HOST_CHANGE_REQUEST == command.getType(),
                String.format("invalid command type : %s", command.getType()));
        WorkflowHostChangeRequest workflowHostChangeRequest =
                JSONUtils.parseObject(command.getBody(), WorkflowHostChangeRequest.class);
        if (workflowHostChangeRequest == null) {
            logger.error("host update command is null");
            return;
        }
        logger.info("Received workflow host change command : {}", workflowHostChangeRequest);
        try {
            LoggerUtils.setTaskInstanceIdMDC(workflowHostChangeRequest.getTaskInstanceId());
            WorkflowHostChangeResponse workflowHostChangeResponse;
            TaskExecutionContext taskExecutionContext =
                    TaskExecutionContextCacheManager.getByTaskInstanceId(workflowHostChangeRequest.getTaskInstanceId());
            if (taskExecutionContext != null) {
                taskExecutionContext.setMasterHost(workflowHostChangeRequest.getWorkflowHost());
                messageRetryRunner.updateMessageHost(workflowHostChangeRequest.getTaskInstanceId(),
                        workflowHostChangeRequest.getWorkflowHost());
                workflowHostChangeResponse = WorkflowHostChangeResponse.success();
                logger.info("Success update workflow host, taskInstanceId : {}, workflowHost: {}",
                        workflowHostChangeRequest.getTaskInstanceId(), workflowHostChangeRequest.getWorkflowHost());
            } else {
                workflowHostChangeResponse = WorkflowHostChangeResponse.failed();
                logger.error("Cannot find the taskExecutionContext, taskInstanceId : {}",
                        workflowHostChangeRequest.getTaskInstanceId());
            }
            channel.writeAndFlush(workflowHostChangeResponse.convert2Command(command.getOpaque())).addListener(
                    (ChannelFutureListener) channelFuture -> {
                        if (!channelFuture.isSuccess()) {
                            logger.error("send host update response failed");
                        }
                    });
        } finally {
            LoggerUtils.removeTaskInstanceIdMDC();
        }
    }

}
