package org.apache.dolphinscheduler.alert.content.template;

import org.apache.dolphinscheduler.alert.config.AlertConfig;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public abstract class BaseAlertTemplateInjector implements AlertTemplateInjector {

    protected final AlertTemplate alertTemplate;

    public BaseAlertTemplateInjector(AlertConfig alertConfig) {
        final Properties properties = new Properties();
        try (
                InputStream fis = BaseAlertTemplateInjector.class
                        .getResourceAsStream("/" + alertConfig.getTemplate().getFile())) {
            properties.load(fis);
        } catch (IOException e) {
            throw new IllegalArgumentException(
                    "Load alert template file error, alertTemplate: " + alertConfig.getTemplate());
        }
        this.alertTemplate = AlertTemplate.builder()
                .titleTemplate(properties.getProperty(getAlertType().name() + ".title"))
                .contentTemplate(properties.getProperty(getAlertType().name() + ".content"))
                .build();
    }

}
