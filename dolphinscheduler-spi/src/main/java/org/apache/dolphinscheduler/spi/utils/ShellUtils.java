package org.apache.dolphinscheduler.spi.utils;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import lombok.experimental.UtilityClass;

@UtilityClass
public class ShellUtils {

    public static final List<String> ENV_SOURCE_LIST = Arrays.stream(
            PropertyUtils.getString("shell.env_source_list", "/etc/profile,~/.bash_profile").split(","))
            .map(String::trim)
            .collect(Collectors.toList());

}
