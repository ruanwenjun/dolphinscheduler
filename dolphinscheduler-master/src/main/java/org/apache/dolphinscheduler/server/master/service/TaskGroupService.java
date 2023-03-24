package org.apache.dolphinscheduler.server.master.service;

import org.apache.dolphinscheduler.dao.entity.ProcessInstance;
import org.apache.dolphinscheduler.dao.entity.TaskInstance;
import org.apache.dolphinscheduler.service.process.ProcessService;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class TaskGroupService {

    @Autowired
    private ProcessService processService;

    public void releaseTaskGroup(TaskInstance taskInstance) {
        if (taskInstance.getTaskGroupId() > 0) {
            TaskInstance nextTaskInstance = this.processService.releaseTaskGroup(taskInstance);
            if (nextTaskInstance != null) {
                ProcessInstance processInstance =
                        this.processService.findProcessInstanceById(nextTaskInstance.getProcessInstanceId());
                this.processService.sendStartTask2Master(processInstance,
                        nextTaskInstance.getId(),
                        org.apache.dolphinscheduler.remote.command.CommandType.TASK_WAKEUP_EVENT_REQUEST);
            }
        }
    }
}
