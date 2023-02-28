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

package org.apache.dolphinscheduler.common.utils;

import static org.apache.dolphinscheduler.common.Constants.HADOOP_SECURITY_AUTHENTICATION_STARTUP_STATE;
import static org.apache.dolphinscheduler.common.Constants.RESOURCE_STORAGE_TYPE;

import org.apache.dolphinscheduler.common.Constants;
import org.apache.dolphinscheduler.spi.enums.ResUploadType;

/**
 * common utils
 */
public class CommonUtils {

    protected CommonUtils() {
        throw new UnsupportedOperationException("Construct CommonUtils");
    }

    /**
     * @return is develop mode
     */
    public static boolean isDevelopMode() {
        return PropertyUtils.getBoolean(Constants.DEVELOPMENT_STATE, true);
    }

    /**
     * @return sudo enable
     */
    public static boolean isSudoEnable() {
        return PropertyUtils.getBoolean(Constants.SUDO_ENABLE, true);
    }

    public static boolean isSetTenantOwnerEnable() {
        return PropertyUtils.getBoolean(Constants.SET_TENANT_OWNER_ENABLE, false);
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
