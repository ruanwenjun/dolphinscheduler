package org.apache.dolphinscheduler.plugin.task.remoteshell;

import lombok.Data;

import org.apache.dolphinscheduler.plugin.task.api.enums.ResourceType;
import org.apache.dolphinscheduler.plugin.task.api.parameters.AbstractParameters;
import org.apache.dolphinscheduler.plugin.task.api.parameters.resource.ResourceParametersHelper;

@Data
public class RemoteShellParameters extends AbstractParameters {

    private String rawScript;

    private String type;

    /**
     * datasource id
     */
    private int datasource;

    @Override
    public boolean checkParameters() {
        return rawScript != null && !rawScript.isEmpty();
    }

    @Override
    public ResourceParametersHelper getResources() {
        ResourceParametersHelper resources = super.getResources();
        resources.put(ResourceType.DATASOURCE, datasource);
        return resources;
    }

}
