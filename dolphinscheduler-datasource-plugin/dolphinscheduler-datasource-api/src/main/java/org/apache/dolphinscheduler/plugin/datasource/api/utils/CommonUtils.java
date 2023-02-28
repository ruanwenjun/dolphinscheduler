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

package org.apache.dolphinscheduler.plugin.datasource.api.utils;

import static org.apache.dolphinscheduler.plugin.task.api.TaskConstants.DATA_QUALITY_JAR_NAME;
import static org.apache.dolphinscheduler.plugin.task.api.TaskConstants.RESOURCE_UPLOAD_PATH;
import static org.apache.dolphinscheduler.spi.utils.Constants.HADOOP_SECURITY_AUTHENTICATION_STARTUP_STATE;
import static org.apache.dolphinscheduler.spi.utils.Constants.RESOURCE_STORAGE_TYPE;

import org.apache.dolphinscheduler.spi.enums.ResUploadType;
import org.apache.dolphinscheduler.spi.utils.PropertyUtils;

/**
 * common utils
 */
public class CommonUtils {

    private CommonUtils() {
        throw new UnsupportedOperationException("Construct CommonUtils");
    }

    public static String getDataQualityJarName() {
        String dqsJarName = PropertyUtils.getString(DATA_QUALITY_JAR_NAME);

        if (org.apache.commons.lang.StringUtils.isEmpty(dqsJarName)) {
            return "dolphinscheduler-data-quality.jar";
        }

        return dqsJarName;
    }

    /**
     * hdfs udf dir
     *
     * @param tenantCode tenant code
     * @return get udf dir on hdfs
     */
    public static String getHdfsUdfDir(String tenantCode) {
        return String.format("%s/udfs", getHdfsTenantDir(tenantCode));
    }

    /**
     * @param tenantCode tenant code
     * @return file directory of tenants on hdfs
     */
    public static String getHdfsTenantDir(String tenantCode) {
        return String.format("%s/%s", getHdfsDataBasePath(), tenantCode);
    }

    /**
     * get data hdfs path
     *
     * @return data hdfs path
     */
    public static String getHdfsDataBasePath() {
        String resourceUploadPath = PropertyUtils.getString(RESOURCE_UPLOAD_PATH, "/dolphinscheduler");
        if ("/".equals(resourceUploadPath)) {
            // if basepath is configured to /, the generated url may be //default/resources (with extra leading /)
            return "";
        } else {
            return resourceUploadPath;
        }
    }

    /**
     * if upload resource is HDFS and kerberos startup is true , else false
     *
     * @return true if upload resource is HDFS and kerberos startup
     */
    public static boolean getKerberosStartupState() {
        String resUploadStartupType = PropertyUtils.getUpperCaseString(RESOURCE_STORAGE_TYPE);
        ResUploadType resUploadType = ResUploadType.valueOf(resUploadStartupType);
        Boolean kerberosStartupState = PropertyUtils.getBoolean(HADOOP_SECURITY_AUTHENTICATION_STARTUP_STATE, false);
        return resUploadType == ResUploadType.HDFS && kerberosStartupState;
    }
}
