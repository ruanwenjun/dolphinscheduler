package org.apache.dolphinscheduler.alert.content.template;

import org.apache.dolphinscheduler.alert.config.AlertConfig;
import org.apache.dolphinscheduler.alert.content.DefaultAlertContentWrapperGenerator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;

// @ConditionalOnProperty(prefix = "alert.template", name = "enable", havingValue = "true")
// public class TemplateConfiguration {
//
// @Autowired
// private AlertConfig alertConfig;
//
// @Bean
// public DefaultAlertContentWrapperGenerator defaultAlertContentWrapperGenerator() {
// return new DefaultAlertContentWrapperGenerator();
// }
//
// @Bean
// public CloseAlertContentTemplateInjector closeAlertContentTemplateInjector() {
// return new CloseAlertContentTemplateInjector(alertConfig);
// }
//
// @Bean
// public DataQualityTaskResultAlertContentTemplateInjector dataQualityTaskResultAlertContentTemplateInjector() {
// return new DataQualityTaskResultAlertContentTemplateInjector(alertConfig);
// }
//
// @Bean
// public TaskFailureTemplateAlertContentInjector taskFailureTemplateAlertContentInjector() {
// return new TaskFailureTemplateAlertContentInjector(alertConfig);
// }
//
// @Bean
// public TaskFaultToleranceTemplateAlertContentInjector taskFaultToleranceTemplateAlertContentInjector() {
// return new TaskFaultToleranceTemplateAlertContentInjector(alertConfig);
// }
//
// @Bean
// public TaskResultTemplateAlertContentInjector taskResultTemplateAlertContentInjector() {
// return new TaskResultTemplateAlertContentInjector(alertConfig);
// }
//
// @Bean
// public TaskSuccessTemplateAlertContentInjector taskSuccessTemplateAlertContentInjector() {
// return new TaskSuccessTemplateAlertContentInjector(alertConfig);
// }
//
// @Bean
// public TaskTimeoutTemplateAlertContentInjector taskTimeoutTemplateAlertContentInjector() {
// return new TaskTimeoutTemplateAlertContentInjector(alertConfig);
// }
//
// @Bean
// public WorkflowFaultToleranceTemplateAlertContentInjector workflowFaultToleranceTemplateAlertContentInjector() {
// return new WorkflowFaultToleranceTemplateAlertContentInjector(alertConfig);
// }
//
// @Bean
// public WorkflowTimeCheckNotRunAlertTemplateInjector workflowTimeCheckNotRunAlertTemplateInjector() {
// return new WorkflowTimeCheckNotRunAlertTemplateInjector(alertConfig);
// }
//
// @Bean
// public WorkflowTimeCheckStillRunningAlertTemplateInjector workflowTimeCheckStillRunningAlertTemplateInjector() {
// return new WorkflowTimeCheckStillRunningAlertTemplateInjector(alertConfig);
// }
// }
