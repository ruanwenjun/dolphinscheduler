package org.apache.dolphinscheduler.common.utils;

import static org.apache.dolphinscheduler.common.Constants.HADOOP_SECURITY_AUTHENTICATION_STARTUP_STATE;
import static org.apache.dolphinscheduler.common.Constants.JAVA_SECURITY_KRB5_CONF_PATH;
import static org.apache.dolphinscheduler.common.Constants.LOGIN_USER_KEY_TAB_PATH;
import static org.apache.dolphinscheduler.common.Constants.LOGIN_USER_KEY_TAB_USERNAME;
import static org.apache.dolphinscheduler.common.Constants.RESOURCE_STORAGE_TYPE;

import org.apache.dolphinscheduler.plugin.task.api.utils.KerberosUtils;
import org.apache.dolphinscheduler.spi.utils.PropertyUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PrepareForTest(value = {PropertyUtils.class, UserGroupInformation.class, CommonUtils.class})
public class KerberosUtilsTest {

    @Test
    public void testGetKerberosStartupState() {
        PowerMockito.mockStatic(CommonUtils.class);
        PowerMockito.when(CommonUtils.getKerberosStartupState()).thenReturn(false);
        boolean kerberosStartupState = CommonUtils.getKerberosStartupState();
        Assert.assertFalse(kerberosStartupState);

        PowerMockito.mockStatic(PropertyUtils.class);
        PowerMockito.when(PropertyUtils.getUpperCaseString(RESOURCE_STORAGE_TYPE))
                .thenReturn("HDFS");
        PowerMockito
                .when(PropertyUtils
                        .getBoolean(HADOOP_SECURITY_AUTHENTICATION_STARTUP_STATE, true))
                .thenReturn(Boolean.TRUE);
        kerberosStartupState = CommonUtils.getKerberosStartupState();
        Assert.assertFalse(kerberosStartupState);
    }

    @Test
    public void testLoadKerberosConf() {
        try {
            PowerMockito.mockStatic(PropertyUtils.class);
            PowerMockito
                    .when(PropertyUtils.getUpperCaseString(RESOURCE_STORAGE_TYPE))
                    .thenReturn("HDFS");
            PowerMockito
                    .when(PropertyUtils
                            .getBoolean(HADOOP_SECURITY_AUTHENTICATION_STARTUP_STATE, false))
                    .thenReturn(Boolean.TRUE);
            PowerMockito
                    .when(PropertyUtils.getString(JAVA_SECURITY_KRB5_CONF_PATH))
                    .thenReturn("/opt/krb5.conf");
            PowerMockito
                    .when(PropertyUtils.getString(LOGIN_USER_KEY_TAB_USERNAME))
                    .thenReturn("hdfs-mycluster@ESZ.COM");
            PowerMockito.when(PropertyUtils.getString(LOGIN_USER_KEY_TAB_PATH))
                    .thenReturn("/opt/hdfs.headless.keytab");

            PowerMockito.mockStatic(UserGroupInformation.class);
            KerberosUtils.loadKerberosConf(new Configuration());
        } catch (Exception e) {
            Assert.fail("load Kerberos Conf failed：" + e.getMessage());
        }
    }
}
