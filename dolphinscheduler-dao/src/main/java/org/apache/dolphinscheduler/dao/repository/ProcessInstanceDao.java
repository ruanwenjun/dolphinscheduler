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

package org.apache.dolphinscheduler.dao.repository;

import lombok.NonNull;
import org.apache.dolphinscheduler.dao.entity.ProcessInstance;
import org.apache.dolphinscheduler.plugin.task.api.enums.ExecutionStatus;

import java.util.List;
import java.util.Optional;

public interface ProcessInstanceDao {

    List<ProcessInstance> queryProcessInstanceByIds(List<Integer> processInstanceIds);

    List<ProcessInstance> queryProcessInstanceByOperationId(Long operationId);

    Optional<ProcessInstance> queryProcessInstanceById(@NonNull Integer processInstanceId);

    Optional<ProcessInstance> queryProcessInstanceByName(String processInstanceName);

    int insertProcessInstance(ProcessInstance processInstance);

    int updateProcessInstance(ProcessInstance processInstance);

    /**
     * insert or update work process instance to database
     *
     * @param processInstance processInstance
     */
    int upsertProcessInstance(ProcessInstance processInstance);

    List<ProcessInstance> queryProcessInstanceByStatus(@NonNull ExecutionStatus pauseByIsolation);

    long countByProcessDefinitionCodes(List<Long> processDefinitionCodes, @NonNull ExecutionStatus runningExecution);
}
