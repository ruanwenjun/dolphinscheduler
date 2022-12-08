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

package org.apache.dolphinscheduler.alert.api.content;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.dolphinscheduler.alert.api.enums.AlertEvent;
import org.apache.dolphinscheduler.alert.api.enums.AlertType;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class ServerAlertContent implements AlertContent {

    /**
     * server type :master or worker
     */
    @JsonProperty("type")
    private String type;
    @JsonProperty("host")
    private String host;
    @JsonProperty("event")
    private AlertEvent event;

    @Override
    public AlertType getAlertType() {
        return AlertType.SERVER_CRASH_ALERT;
    }

    @Override
    public String getProjectName() {
        return "";
    }

    @Override
    public String getWorkflowInstanceName() {
        return "";
    }
}
