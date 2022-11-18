package org.apache.dolphinscheduler.dao.repository;

import lombok.NonNull;
import org.apache.dolphinscheduler.dao.entity.ProcessDefinition;

import java.util.List;
import java.util.Optional;

public interface ProcessDefinitionDao {

    Optional<ProcessDefinition> queryProcessDefinitionByCode(@NonNull Long processDefinitionCode);

    List<Long> selectProcessDefinitionCodeByProjectCode(long projectCode);

    long countByProjectCode(long projectCode);
}
