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

package org.apache.dolphinscheduler.plugin.task.hivecli;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.List;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.apache.dolphinscheduler.plugin.task.api.model.ResourceInfo;

@Slf4j
@UtilityClass
public class HiveSqlScriptReader {

    public static String readHiveSqlContent(String taskWorkingDirectory, HiveCliParameters hiveCliParameters) {
        if (HiveCliConstants.TYPE_FILE.equals(hiveCliParameters.getHiveCliTaskExecutionType())) {
            List<ResourceInfo> resources = hiveCliParameters.getResourceList();
            if (CollectionUtils.isEmpty(resources)) {
                throw new IllegalArgumentException("HiveCliTaskExecutionType is FILE, but resourceList is empty");
            }
            if (resources.size() > 1) {
                log.warn("HiveCliTaskExecutionType is FILE, but resources: {} size > 1, use the first one by default.", resources);
            }
            try {
                return FileUtils.readFileToString(new File(Paths.get(taskWorkingDirectory, resources.get(0).getResourceName()).toString()),
                        StandardCharsets.UTF_8);
            } catch (Exception ex) {
                throw new IllegalArgumentException("Read HiveSql from " + resources.get(0) + " failed", ex);
            }
        } else {
            return hiveCliParameters.getHiveSqlScript();
        }
    }
}
