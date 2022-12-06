package org.apache.dolphinscheduler.api.configuration;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.dolphinscheduler.common.utils.JSONUtils;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.validation.Errors;
import org.springframework.validation.Validator;
import org.springframework.validation.annotation.Validated;

import java.util.Map;
import java.util.Set;

@Data
@Slf4j
@Validated
@Configuration
@ConfigurationProperties(prefix = "api-server")
public class ApiServerConfig implements Validator {

    private Map<String, Set<String>> datasourcePluginIllegalParams;

    @Override
    public boolean supports(Class<?> clazz) {
        return ApiServerConfig.class.isAssignableFrom(clazz);
    }

    @Override
    public void validate(Object target, Errors errors) {
        printConfig();
    }

    private void printConfig() {
        log.info("datasource-plugin-illegal-params: {}", JSONUtils.writeAsPrettyString(datasourcePluginIllegalParams));
    }
}
