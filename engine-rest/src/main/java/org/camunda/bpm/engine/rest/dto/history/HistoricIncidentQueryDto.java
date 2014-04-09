/* Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.camunda.bpm.engine.rest.dto.history;

import java.util.ArrayList;
import java.util.List;

import javax.ws.rs.core.MultivaluedMap;

import org.camunda.bpm.engine.ProcessEngine;
import org.camunda.bpm.engine.history.HistoricIncidentQuery;
import org.camunda.bpm.engine.rest.dto.AbstractQueryDto;
import org.camunda.bpm.engine.rest.dto.CamundaQueryParam;
import org.camunda.bpm.engine.rest.dto.converter.BooleanConverter;

/**
 * @author Roman Smirnov
 *
 */
public class HistoricIncidentQueryDto extends AbstractQueryDto<HistoricIncidentQuery> {

  private static final String SORT_BY_INCIDENT_ID = "incidentId";
  private static final String SORT_BY_CREATE_TIME = "createTime";
  private static final String SORT_BY_END_TIME = "endTime";
  private static final String SORT_BY_INCIDENT_TYPE = "incidentType";
  private static final String SORT_BY_EXECUTION_ID = "executionId";
  private static final String SORT_BY_ACTIVITY_ID = "activityId";
  private static final String SORT_BY_PROCESS_INSTANCE_ID = "processInstanceId";
  private static final String SORT_BY_PROCESS_DEFINITION_ID = "processDefinitionId";
  private static final String SORT_BY_CAUSE_INCIDENT_ID = "causeIncidentId";
  private static final String SORT_BY_ROOT_CAUSE_INCIDENT_ID = "rootCauseIncidentId";
  private static final String SORT_BY_CONFIGURATION = "configuration";

  private static final List<String> VALID_SORT_BY_VALUES;
  static {
    VALID_SORT_BY_VALUES = new ArrayList<String>();
    VALID_SORT_BY_VALUES.add(SORT_BY_INCIDENT_ID);
    VALID_SORT_BY_VALUES.add(SORT_BY_CREATE_TIME);
    VALID_SORT_BY_VALUES.add(SORT_BY_END_TIME);
    VALID_SORT_BY_VALUES.add(SORT_BY_INCIDENT_TYPE);
    VALID_SORT_BY_VALUES.add(SORT_BY_EXECUTION_ID);
    VALID_SORT_BY_VALUES.add(SORT_BY_ACTIVITY_ID);
    VALID_SORT_BY_VALUES.add(SORT_BY_PROCESS_INSTANCE_ID);
    VALID_SORT_BY_VALUES.add(SORT_BY_PROCESS_DEFINITION_ID);
    VALID_SORT_BY_VALUES.add(SORT_BY_CAUSE_INCIDENT_ID);
    VALID_SORT_BY_VALUES.add(SORT_BY_ROOT_CAUSE_INCIDENT_ID);
    VALID_SORT_BY_VALUES.add(SORT_BY_CONFIGURATION);
  }

  protected String incidentId;
  protected String incidentType;
  protected String incidentMessage;
  protected String processDefinitionId;
  protected String processInstanceId;
  protected String executionId;
  protected String activityId;
  protected String causeIncidentId;
  protected String rootCauseIncidentId;
  protected String configuration;
  protected Boolean open;
  protected Boolean resolved;
  protected Boolean deleted;

  public HistoricIncidentQueryDto() {}

  public HistoricIncidentQueryDto(MultivaluedMap<String, String> queryParameters) {
    super(queryParameters);
  }

  @CamundaQueryParam("incidentId")
  public void setIncidentId(String incidentId) {
    this.incidentId = incidentId;
  }

  @CamundaQueryParam("incidentType")
  public void setIncidentType(String incidentType) {
    this.incidentType = incidentType;
  }

  @CamundaQueryParam("incidentMessage")
  public void setIncidentMessage(String incidentMessage) {
    this.incidentMessage = incidentMessage;
  }

  @CamundaQueryParam("processDefinitionId")
  public void setProcessDefinitionId(String processDefinitionId) {
    this.processDefinitionId = processDefinitionId;
  }

  @CamundaQueryParam("processInstanceId")
  public void setProcessInstanceId(String processInstanceId) {
    this.processInstanceId = processInstanceId;
  }

  @CamundaQueryParam("executionId")
  public void setExecutionId(String executionId) {
    this.executionId = executionId;
  }

  @CamundaQueryParam("activityId")
  public void setActivityId(String activityId) {
    this.activityId = activityId;
  }

  @CamundaQueryParam("causeIncidentId")
  public void setCauseIncidentId(String causeIncidentId) {
    this.causeIncidentId = causeIncidentId;
  }

  @CamundaQueryParam("rootCauseIncidentId")
  public void setRootCauseIncidentId(String rootCauseIncidentId) {
    this.rootCauseIncidentId = rootCauseIncidentId;
  }

  @CamundaQueryParam("configuration")
  public void setConfiguration(String configuration) {
    this.configuration = configuration;
  }

  @CamundaQueryParam(value = "open", converter = BooleanConverter.class)
  public void setOpen(Boolean open) {
    this.open = open;
  }

  @CamundaQueryParam(value = "resolved", converter = BooleanConverter.class)
  public void setResolved(Boolean resolved) {
    this.resolved = resolved;
  }

  @CamundaQueryParam(value = "deleted", converter = BooleanConverter.class)
  public void setDeleted(Boolean deleted) {
    this.deleted = deleted;
  }

  @Override
  protected boolean isValidSortByValue(String value) {
    return VALID_SORT_BY_VALUES.contains(value);
  }

  @Override
  protected HistoricIncidentQuery createNewQuery(ProcessEngine engine) {
    return engine.getHistoryService().createHistoricIncidentQuery();
  }

  @Override
  protected void applyFilters(HistoricIncidentQuery query) {
    if (incidentId != null) {
      query.incidentId(incidentId);
    }
    if (incidentType != null) {
      query.incidentType(incidentType);
    }
    if (incidentMessage != null) {
      query.incidentMessage(incidentMessage);
    }
    if (processDefinitionId != null) {
      query.processDefinitionId(processDefinitionId);
    }
    if (processInstanceId != null) {
      query.processInstanceId(processInstanceId);
    }
    if (executionId != null) {
      query.executionId(executionId);
    }
    if (activityId != null) {
      query.activityId(activityId);
    }
    if (causeIncidentId != null) {
      query.causeIncidentId(causeIncidentId);
    }
    if (rootCauseIncidentId != null) {
      query.rootCauseIncidentId(rootCauseIncidentId);
    }
    if (configuration != null) {
      query.configuration(configuration);
    }
    if (open != null) {
      query.open();
    }
    if (resolved != null && resolved) {
      query.resolved();
    }
    if (deleted != null && deleted) {
      query.deleted();
    }
  }

  @Override
  protected void applySortingOptions(HistoricIncidentQuery query) {
    if (sortBy != null) {
      if (sortBy.equals(SORT_BY_INCIDENT_ID)) {
        query.orderByIncidentId();
      } else if (sortBy.equals(SORT_BY_CREATE_TIME)) {
        query.orderByCreateTime();
      } else if (sortBy.equals(SORT_BY_END_TIME)) {
        query.orderByEndTime();
      } else if (sortBy.equals(SORT_BY_INCIDENT_TYPE)) {
        query.orderByIncidentType();
      } else if (sortBy.equals(SORT_BY_EXECUTION_ID)) {
        query.orderByExecutionId();
      } else if (sortBy.equals(SORT_BY_ACTIVITY_ID)) {
        query.orderByActivityId();
      } else if (sortBy.equals(SORT_BY_PROCESS_INSTANCE_ID)) {
        query.orderByProcessInstanceId();
      } else if (sortBy.equals(SORT_BY_PROCESS_DEFINITION_ID)) {
        query.orderByProcessDefinitionId();
      } else if (sortBy.equals(SORT_BY_CAUSE_INCIDENT_ID)) {
        query.orderByCauseIncidentId();
      } else if (sortBy.equals(SORT_BY_ROOT_CAUSE_INCIDENT_ID)) {
        query.orderByRootCauseIncidentId();
      } else if (sortBy.equals(SORT_BY_CONFIGURATION)) {
        query.orderByConfiguration();
      }
    }

    if (sortOrder != null) {
      if (sortOrder.equals(SORT_ORDER_ASC_VALUE)) {
        query.asc();
      } else if (sortOrder.equals(SORT_ORDER_DESC_VALUE)) {
        query.desc();
      }
    }
  }

}