/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.dolphinscheduler.alert.api.enums;

import com.baomidou.mybatisplus.annotation.EnumValue;

import java.util.HashMap;
import java.util.Map;

/**
 * describe the reason why alert generates
 */
public enum AlertType {

    /**
     * 0 process instance failure, 1 process instance success, 2 process instance blocked, 3 process instance timeout, 4 fault tolerance warning,
     * 5 task failure, 6 task success, 7 task timeout, 8 close alert
      */
    PROCESS_INSTANCE_FAILURE(0, "WorkflowInstanceFailure", "工作流执行失败"),
    PROCESS_INSTANCE_SUCCESS(1, "WorkflowInstanceSuccess", "工作流执行成功"),
    PROCESS_INSTANCE_BLOCKED(2, "WorkflowInstanceBlocked", "工作流执行阻塞"),
    PROCESS_INSTANCE_TIMEOUT(3, "WorkflowInstanceTimeout", "工作流执行超时"),
    @Deprecated
    FAULT_TOLERANCE_WARNING(4, "FaultToleranceWarning", "容错"),
    TASK_FAILURE(5, "TaskFailure", "任务执行失败"),
    TASK_SUCCESS(6, "TaskSuccess", "任务执行成功"),
    TASK_TIMEOUT(7, "TaskTimeout", "任务执行超时"),

    CLOSE_ALERT(8, "The workflow instance success, can close the before alert", "关闭告警"),

    WORKFLOW_FAULT_TOLERANCE(9, "WorkflowFaultTolerance", "容错"),
    TASK_FAULT_TOLERANCE(10, "TaskFaultTolerance", "容错"),

    TASK_RESULT(11, "TaskResult", "任务结果"),

    DATA_QUALITY_TASK_RESULT(12, "DataQualityTaskResult", "数据质量任务结果"),

    WORKFLOW_TIME_CHECK_NOT_RUN_ALERT(13, "WorkflowTimeCheckNotRunAlert", "工作流未执行"),
    WORKFLOW_TIME_CHECK_STILL_RUNNING_ALERT(14, "WorkflowTimeCheckStillRunningRunAlert", "工作流仍在运行"),

    SERVER_CRASH_ALERT(15, "ServerCrashAlert", "服务下线"),
    ;

    private static final Map<Integer, AlertType> alertTypeMap = new HashMap<>();

    static {
        for (AlertType alertType : AlertType.values()) {
            alertTypeMap.put(alertType.getCode(), alertType);
        }
    }

    AlertType(int code, String descp, String descpCN) {
        this.code = code;
        this.descp = descp;
        this.descpCN = descpCN;
    }

    @EnumValue
    private final int code;
    private final String descp;
    private final String descpCN;

    public int getCode() {
        return code;
    }

    public String getDescp() {
        return descp;
    }

    public String getDescpCN() {
        return descpCN;
    }

    public static AlertType ofCode(int code) {
        return alertTypeMap.get(code);
    }
}
