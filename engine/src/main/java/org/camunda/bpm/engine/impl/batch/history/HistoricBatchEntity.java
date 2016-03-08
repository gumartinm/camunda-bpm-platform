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
package org.camunda.bpm.engine.impl.batch.history;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.camunda.bpm.engine.batch.history.HistoricBatch;
import org.camunda.bpm.engine.impl.context.Context;
import org.camunda.bpm.engine.impl.db.DbEntity;
import org.camunda.bpm.engine.impl.db.HasDbRevision;
import org.camunda.bpm.engine.impl.history.event.HistoryEvent;
import org.camunda.bpm.engine.impl.interceptor.CommandContext;
import org.camunda.bpm.engine.impl.persistence.entity.HistoricJobLogManager;

/**
 * @author Thorben Lindhauer
 *
 */
public class HistoricBatchEntity extends HistoryEvent implements HistoricBatch, DbEntity {

  private static final long serialVersionUID = 1L;

  protected String id;
  protected String type;
  protected int size;
  protected int numberOfJobsPerSeedJobInvocation;
  protected int numberOfInvocationsPerJob;
  protected String seedJobDefinitionId;
  protected String executionJobDefinitionId;
  protected Date startTime;
  protected Date endTime;

  @Override
  public Object getPersistentState() {
    Map<String, Object> persistentState = new HashMap<String, Object>();

    persistentState.put("endTime", endTime);

    return persistentState;
  }

  public String getType() {
    return type;
  }

  public int getSize() {
    return size;
  }

  public int getNumberOfInvocationsPerJob() {
    return numberOfInvocationsPerJob;
  }

  public void setNumberOfInvocationsPerJob(int numberOfInvocationsPerJob) {
    this.numberOfInvocationsPerJob = numberOfInvocationsPerJob;
  }

  public int getNumberOfJobsPerSeedJobInvocation() {
    return numberOfJobsPerSeedJobInvocation;
  }

  public void setNumberOfJobsPerSeedJobInvocation(int numberOfJobsPerSeedJobInvocation) {
    this.numberOfJobsPerSeedJobInvocation = numberOfJobsPerSeedJobInvocation;
  }

  public String getSeedJobDefinitionId() {
    return seedJobDefinitionId;
  }

  public void setSeedJobDefinitionId(String seedJobDefinitionId) {
    this.seedJobDefinitionId = seedJobDefinitionId;
  }

  public String getExecutionJobDefinitionId() {
    return executionJobDefinitionId;
  }

  public void setExecutionJobDefinitionId(String executionJobDefinitionId) {
    this.executionJobDefinitionId = executionJobDefinitionId;
  }

  public Date getStartTime() {
    return startTime;
  }

  public Date getEndTime() {
    return endTime;
  }

  public void setType(String type) {
    this.type = type;
  }

  public void setSize(int size) {
    this.size = size;
  }

  public void setStartTime(Date startTime) {
    this.startTime = startTime;
  }

  public void setEndTime(Date endTime) {
    this.endTime = endTime;
  }

  public void delete() {
    CommandContext commandContext = Context.getCommandContext();
    HistoricJobLogManager historicJobLogManager = commandContext.getHistoricJobLogManager();
    historicJobLogManager.deleteHistoricJobLogsByJobDefinitionId(seedJobDefinitionId);
    historicJobLogManager.deleteHistoricJobLogsByJobDefinitionId(executionJobDefinitionId);
    commandContext.getHistoricBatchManager().delete(this);
  }

}
