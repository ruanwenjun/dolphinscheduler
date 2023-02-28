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

package org.apache.dolphinscheduler.plugin.datasource.hive;

import static org.apache.dolphinscheduler.spi.utils.Constants.HADOOP_SECURITY_AUTHENTICATION_STARTUP_STATE;
import static org.apache.dolphinscheduler.spi.utils.Constants.JAVA_SECURITY_KRB5_CONF;
import static org.apache.dolphinscheduler.spi.utils.Constants.JAVA_SECURITY_KRB5_CONF_PATH;
import static org.apache.dolphinscheduler.spi.utils.Constants.LOGIN_USER_KEY_TAB_PATH;
import static org.apache.dolphinscheduler.spi.utils.Constants.LOGIN_USER_KEY_TAB_USERNAME;

import org.apache.dolphinscheduler.plugin.datasource.api.client.CommonDataSourceClient;
import org.apache.dolphinscheduler.plugin.datasource.api.provider.JDBCDataSourceProvider;
import org.apache.dolphinscheduler.plugin.datasource.api.utils.CommonUtils;
import org.apache.dolphinscheduler.plugin.datasource.hive.param.HiveConnectionParam;
import org.apache.dolphinscheduler.plugin.task.api.utils.KerberosUtils;
import org.apache.dolphinscheduler.spi.datasource.BaseConnectionParam;
import org.apache.dolphinscheduler.spi.enums.DbType;
import org.apache.dolphinscheduler.spi.utils.PropertyUtils;
import org.apache.dolphinscheduler.spi.utils.StringUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.lang.reflect.Field;
import java.sql.Connection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;

import sun.security.krb5.Config;

public class HiveDataSourceClient extends CommonDataSourceClient {

    private static final Logger logger = LoggerFactory.getLogger(HiveDataSourceClient.class);

    private Configuration hadoopConf;

    public HiveDataSourceClient(BaseConnectionParam baseConnectionParam, DbType dbType) {
        super(baseConnectionParam, dbType);
    }

    @Override
    protected void preInit() {
        logger.info("PreInit in {}", getClass().getName());
    }

    @Override
    protected void initClient(BaseConnectionParam baseConnectionParam, DbType dbType) {
        logger.info("Create Configuration for hive configuration.");
        this.hadoopConf = createHadoopConf();
        logger.info("Create Configuration success.");

        logger.info("Create UserGroupInformation.");
        createUserGroupInformation((HiveConnectionParam) baseConnectionParam);
        logger.info("Create ugi success.");

        this.dataSource = JDBCDataSourceProvider.createOneSessionJdbcDataSource(baseConnectionParam, dbType);
        this.jdbcTemplate = new JdbcTemplate(dataSource);
        logger.info("Init {} success.", getClass().getName());
    }

    @Override
    protected void checkEnv(BaseConnectionParam baseConnectionParam) {
        super.checkEnv(baseConnectionParam);
        checkKerberosEnv();
    }

    private void checkKerberosEnv() {
        HiveConnectionParam connectionParam = (HiveConnectionParam) baseConnectionParam;
        String krb5File = PropertyUtils.getString(connectionParam.getJavaSecurityKrb5Conf(),
            PropertyUtils.getString(JAVA_SECURITY_KRB5_CONF_PATH));
        Boolean kerberosStartupState = PropertyUtils.getBoolean(HADOOP_SECURITY_AUTHENTICATION_STARTUP_STATE, false);
        if (kerberosStartupState && StringUtils.isNotBlank(krb5File)) {
            System.setProperty(JAVA_SECURITY_KRB5_CONF, krb5File);
            try {
                Config.refresh();
                Class<?> kerberosName = Class.forName("org.apache.hadoop.security.authentication.util.KerberosName");
                Field field = kerberosName.getDeclaredField("defaultRealm");
                field.setAccessible(true);
                field.set(null, Config.getInstance().getDefaultRealm());
            } catch (Exception e) {
                throw new RuntimeException("Update Kerberos environment failed.", e);
            }
        }
    }

    private void createUserGroupInformation(HiveConnectionParam connectionParam) {
        String krb5File = PropertyUtils.getString(connectionParam.getJavaSecurityKrb5Conf(),
            PropertyUtils.getString(JAVA_SECURITY_KRB5_CONF_PATH));
        String keytab = PropertyUtils.getString(connectionParam.getLoginUserKeytabPath(),
            PropertyUtils.getString(LOGIN_USER_KEY_TAB_PATH));
        String keytabUser = PropertyUtils.getString(connectionParam.getLoginUserKeytabUsername(),
            PropertyUtils.getString(LOGIN_USER_KEY_TAB_USERNAME));

        try {
            if (CommonUtils.getKerberosStartupState()) {
                KerberosUtils.loadKerberosConf(krb5File, keytabUser, keytab, getHadoopConf());
            } else {
                UserGroupInformation.createRemoteUser(connectionParam.getUser());
            }
        } catch (IOException e) {
            logger.error("Kerberos login fail, krb5File:{}, kertab:{}, keytabUser:{}", krb5File, keytab, keytabUser, e);
            throw new RuntimeException("Kerberos login fail", e);
        }
    }

    protected Configuration createHadoopConf() {
        Configuration hadoopConf = new Configuration();
        hadoopConf.setBoolean("ipc.client.fallback-to-simple-auth-allowed", true);
        return hadoopConf;
    }

    protected Configuration getHadoopConf() {
        return this.hadoopConf;
    }

    @Override
    public Connection getConnection() {
        HiveConnectionParam connectionParam = (HiveConnectionParam) baseConnectionParam;

        String krb5File = PropertyUtils.getString(connectionParam.getJavaSecurityKrb5Conf(),
            PropertyUtils.getString(JAVA_SECURITY_KRB5_CONF_PATH));
        String keytab = PropertyUtils.getString(connectionParam.getLoginUserKeytabPath(),
            PropertyUtils.getString(LOGIN_USER_KEY_TAB_PATH));
        String keytabUser = PropertyUtils.getString(connectionParam.getLoginUserKeytabUsername(),
            PropertyUtils.getString(LOGIN_USER_KEY_TAB_USERNAME));

        try {
            return KerberosUtils.doWithReloadKerberosConfIfAuthFail(() -> dataSource.getConnection(), krb5File,
                keytabUser, keytab, hadoopConf);
        } catch (Exception e) {
            logger.error("Get connection fail", e);
            return null;
        }
    }

    @Override
    public void close() {
        super.close();
        logger.info("Closed Hive datasource client.");
    }
}
