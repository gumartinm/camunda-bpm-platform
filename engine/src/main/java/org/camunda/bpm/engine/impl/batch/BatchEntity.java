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
package org.camunda.bpm.engine.impl.batch;

import java.util.Date;
import java.util.HashMap;
import java.util.List;

import org.camunda.bpm.engine.batch.Batch;
import org.camunda.bpm.engine.impl.context.Context;
import org.camunda.bpm.engine.impl.db.DbEntity;
import org.camunda.bpm.engine.impl.db.HasDbRevision;
import org.camunda.bpm.engine.impl.interceptor.CommandContext;
import org.camunda.bpm.engine.impl.persistence.entity.HistoricJobLogManager;
import org.camunda.bpm.engine.impl.persistence.entity.JobDefinitionEntity;
import org.camunda.bpm.engine.impl.persistence.entity.JobDefinitionManager;
import org.camunda.bpm.engine.impl.persistence.entity.JobEntity;
import org.camunda.bpm.engine.impl.persistence.entity.Nameable;
import org.camunda.bpm.engine.impl.persistence.entity.util.ByteArrayField;
import org.camunda.bpm.engine.impl.util.ClockUtil;

/**
 * @author Thorben Lindhauer
 *
 */
public class BatchEntity implements Batch, DbEntity, Nameable, HasDbRevision {

  public static final BatchSeedJobDeclaration BATCH_SEED_JOB_DECLARATION = new BatchSeedJobDeclaration();

  // persistent
  protected String id;
  protected String type;
  protected int size;
  protected int numberOfJobsPerSeedJobInvocation;
  protected int numberOfInvocationsPerJob;
  protected String seedJobDefinitionId;
  protected String executionJobDefinitionId;
  protected ByteArrayField configuration = new ByteArrayField(this);
  protected int revision;

  // transient
  protected JobDefinitionEntity seedJobDefinition;
  protected JobDefinitionEntity executionJobDefinition;
  protected BatchHandler<?> batchHandler;

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }

  public int getSize() {
    return size;
  }

  public void setSize(int size) {
    this.size = size;
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

  public void setSeedJobDefinitionId(String jobDefinitionId) {
    this.seedJobDefinitionId = jobDefinitionId;
  }

  public String getExecutionJobDefinitionId() {
    return executionJobDefinitionId;
  }

  public void setExecutionJobDefinitionId(String executionJobDefinitionId) {
    this.executionJobDefinitionId = executionJobDefinitionId;
  }

  public JobDefinitionEntity getSeedJobDefinition() {
    if (seedJobDefinition == null && seedJobDefinitionId != null) {
      seedJobDefinition = Context.getCommandContext().getJobDefinitionManager().findById(seedJobDefinitionId);
    }

    return seedJobDefinition;
  }

  public JobDefinitionEntity getExecutionJobDefinition() {
    if (executionJobDefinition == null && executionJobDefinitionId != null) {
      executionJobDefinition = Context.getCommandContext().getJobDefinitionManager().findById(executionJobDefinitionId);
    }

    return executionJobDefinition;
  }

  @Override
  public String getName() {
    return getId();
  }

  public void setConfiguration(String configuration) {
    this.configuration.setByteArrayId(configuration);
  }

  public String getConfiguration() {
    return this.configuration.getByteArrayId();
  }

  public void setConfigurationBytes(byte[] configuration) {
    this.configuration.setByteArrayValue(configuration);
  }

  public byte[] getConfigurationBytes() {
    return this.configuration.getByteArrayValue();
  }

  @Override
  public Object getPersistentState() {
    HashMap<String, Object> persistentState = new HashMap<String, Object>();


    return persistentState;
  }

  public BatchHandler<?> getBatchHandler() {
    if (batchHandler == null) {
      batchHandler = Context.getCommandContext().getProcessEngineConfiguration().getBatchHandler(type);
    }

    return batchHandler;
  }

  public JobDefinitionEntity createSeedJobDefinition() {
    seedJobDefinition = new JobDefinitionEntity(BATCH_SEED_JOB_DECLARATION);
    seedJobDefinition.setJobConfiguration(id);

    Context.getCommandContext().getJobDefinitionManager().insert(seedJobDefinition);

    seedJobDefinitionId = seedJobDefinition.getId();

    return seedJobDefinition;
  }

  public JobDefinitionEntity createExecutionJobDefinition() {

    executionJobDefinition = new JobDefinitionEntity(getBatchHandler().getJobDeclaration());
    // TODO: what to set the configuration to?
//    jobDefinition.setJobConfiguration();
    Context.getCommandContext().getJobDefinitionManager().insert(executionJobDefinition);

    executionJobDefinitionId = executionJobDefinition.getId();

    return executionJobDefinition;
  }

  public JobEntity createSeedJob() {
    JobEntity seedJob = BATCH_SEED_JOB_DECLARATION.createJobInstance(this);

    Context.getCommandContext().getJobManager().insert(seedJob);

    return seedJob;
  }

  public void deleteSeedJob() {
    List<JobEntity> seedJobs = Context.getCommandContext()
      .getJobManager()
      .findJobsByConfiguration(BatchSeedJobHandler.TYPE, id, null);

    if (!seedJobs.isEmpty()) {
      for (JobEntity job : seedJobs) {
        job.delete();
      }
    }
  }

  public JobEntity createMonitorJob() {
    CommandContext commandContext = Context.getCommandContext();
    int pollTime = commandContext.getProcessEngineConfiguration().getBatchCompletionPollWaitTime() * 1000;
    Date dueDate = new Date(ClockUtil.getCurrentTime().getTime() + pollTime);

    // Maybe use an other job declaration
    JobEntity monitorJob = BATCH_SEED_JOB_DECLARATION.createJobInstance(this);
    monitorJob.setDuedate(dueDate);

    commandContext.getJobManager().insertJob(monitorJob);
    return monitorJob;
  }

  public void delete(boolean cascadeToHistory) {
    CommandContext commandContext = Context.getCommandContext();

    deleteSeedJob();
    getBatchHandler().deleteJobs(this);
    commandContext.getBatchManager().delete(this);
    configuration.deleteByteArrayValue();

    JobDefinitionManager jobDefinitionManager = commandContext.getJobDefinitionManager();
    jobDefinitionManager.delete(getSeedJobDefinition());
    jobDefinitionManager.delete(getExecutionJobDefinition());

    fireHistoricEndEvent();

    if (cascadeToHistory) {
      HistoricJobLogManager historicJobLogManager = commandContext.getHistoricJobLogManager();
      historicJobLogManager.deleteHistoricJobLogsByJobDefinitionId(seedJobDefinitionId);
      historicJobLogManager.deleteHistoricJobLogsByJobDefinitionId(executionJobDefinitionId);

      commandContext.getHistoricBatchManager().deleteHistoricBatchById(id);
    }
  }

  public void fireHistoricStartEvent() {
    Context.getCommandContext()
      .getHistoricBatchManager()
      .createHistoricBatch(this);
  }

  public void fireHistoricEndEvent() {
    Context.getCommandContext()
      .getHistoricBatchManager()
      .completeHistoricBatch(this);
  }

  @Override
  public void setRevision(int revision) {
    this.revision = revision;

  }

  @Override
  public int getRevision() {
    return revision;
  }


  @Override
  public int getRevisionNext() {
    return revision + 1;
  }

  public String toString() {
    return "BatchEntity{" +
      "batchHandler=" + batchHandler +
      ", id='" + id + '\'' +
      ", type='" + type + '\'' +
      ", size=" + size +
      ", numberOfJobsPerSeedJobInvocation=" + numberOfJobsPerSeedJobInvocation +
      ", numberOfInvocationsPerJob=" + numberOfInvocationsPerJob +
      ", seedJobDefinitionId='" + seedJobDefinitionId + '\'' +
      ", executionJobDefinitionId='" + executionJobDefinitionId + '\'' +
      ", configurationId='" + configuration.getByteArrayId() + '\'' +
      '}';
  }
}
