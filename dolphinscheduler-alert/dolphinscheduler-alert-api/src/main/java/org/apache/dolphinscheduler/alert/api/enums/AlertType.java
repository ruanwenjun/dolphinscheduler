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
    PROCESS_INSTANCE_FAILURE(0, "WorkflowInstanceFailure"),
    PROCESS_INSTANCE_SUCCESS(1, "WorkflowInstanceSuccess"),
    PROCESS_INSTANCE_BLOCKED(2, "WorkflowInstanceBlocked"),
    PROCESS_INSTANCE_TIMEOUT(3, "WorkflowInstanceTimeout"),
    @Deprecated
    FAULT_TOLERANCE_WARNING(4, "FaultToleranceWarning"),
    TASK_FAILURE(5, "TaskFailure"),
    TASK_SUCCESS(6, "TaskSuccess"),
    TASK_TIMEOUT(7, "TaskTimeout"),

    CLOSE_ALERT(8, "The workflow instance success, can close the before alert"),

    WORKFLOW_FAULT_TOLERANCE(9, "WorkflowFaultTolerance"),
    TASK_FAULT_TOLERANCE(10, "TaskFaultTolerance"),

    TASK_RESULT(11, "TaskResult"),

    DATA_QUALITY_TASK_RESULT(12, "DataQualityTaskResult"),

    WORKFLOW_TIME_CHECK_NOT_RUN_ALERT(13, "WorkflowTimeCheckNotRunAlert"),
    WORKFLOW_TIME_CHECK_STILL_RUNNING_ALERT(14, "WorkflowTimeCheckStillRunningRunAlert"),

    SERVER_CRASH_ALERT(15, "ServerCrashAlert"),
    ;

    private static final Map<Integer, AlertType> alertTypeMap = new HashMap<>();

    static {
        for (AlertType alertType : AlertType.values()) {
            alertTypeMap.put(alertType.getCode(), alertType);
        }
    }

    AlertType(int code, String descp) {
        this.code = code;
        this.descp = descp;
    }

    private final int code;
    private final String descp;

    public int getCode() {
        return code;
    }

    public String getDescp() {
        return descp;
    }

    public static AlertType ofCode(int code) {
        return alertTypeMap.get(code);
    }
}
