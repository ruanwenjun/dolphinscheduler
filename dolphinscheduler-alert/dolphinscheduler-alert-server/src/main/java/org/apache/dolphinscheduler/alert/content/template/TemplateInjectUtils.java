package org.apache.dolphinscheduler.alert.content.template;

import lombok.experimental.UtilityClass;

@UtilityClass
public class TemplateInjectUtils {

    public static final String ALERT_TYPE_TEMPLATE = "\\{\\{alertType}}";
    public static final String PROJECT_NAME_TEMPLATE = "\\{\\{projectName}}";
    public static final String WORKFLOW_INSTANCE_NAME_TEMPLATE = "\\{\\{workflowInstanceName}}";
    public static final String WORKFLOW_NAME_TEMPLATE = "\\{\\{workflowName}}";
    public static final String TASK_NAME_TEMPLATE = "\\{\\{taskName}}";
    public static final String RESULT_TEMPLATE = "\\{\\{result}}";
    public static final String TITLE_TEMPLATE = "\\{\\{title}}";

}
