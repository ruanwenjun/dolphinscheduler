package org.apache.dolphinscheduler.dao.repository;

import lombok.NonNull;
import org.apache.dolphinscheduler.dao.entity.ProcessDefinitionLog;

import java.util.Date;
import java.util.List;
import java.util.Optional;

public interface ProcessDefinitionLogDao {

    Optional<ProcessDefinitionLog> queryProcessDefinitionByCode(@NonNull Long processDefinitionCode,
                                                                @NonNull Integer processDefinitionVersion);

    List<Long> queryProcessDefinitionCodeByProjectCode(long projectCode);
}
