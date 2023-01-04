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

package org.apache.dolphinscheduler.common.enums;

public enum NodeType {

    MASTER("master", "/nodes/master"),
    WORKER("worker", "/nodes/worker"),
    API_SERVER("apiServer", "/nodes/apiServer"),
    API_CLEAN_WORKFLOW_LOCK("apiCleanWorkflowLock", "/lock/apiCleanWorkflowLock"),
    ALERT_SERVER("alertServer", "/nodes/alertServer"),
    ALERT_LOCK("lock", "/lock/alert"),
    ;

    private final String name;
    private final String registryPath;

    NodeType(String name, String registryPath) {
        this.name = name;
        this.registryPath = registryPath;
    }

    public String getName() {
        return name;
    }

    public String getRegistryPath() {
        return registryPath;
    }

}
