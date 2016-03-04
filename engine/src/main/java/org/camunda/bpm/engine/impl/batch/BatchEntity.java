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

import java.util.HashMap;
import java.util.List;

import org.camunda.bpm.engine.batch.Batch;
import org.camunda.bpm.engine.impl.context.Context;
import org.camunda.bpm.engine.impl.db.DbEntity;
import org.camunda.bpm.engine.impl.db.HasDbRevision;
import org.camunda.bpm.engine.impl.persistence.entity.JobDefinitionEntity;
import org.camunda.bpm.engine.impl.persistence.entity.JobEntity;
import org.camunda.bpm.engine.impl.persistence.entity.Nameable;
import org.camunda.bpm.engine.impl.persistence.entity.util.ByteArrayField;

/**
 * @author Thorben Lindhauer
 *
 */
public class BatchEntity implements Batch, DbEntity, Nameable, HasDbRevision {

  public static final BatchSeedJobDeclaration BATCH_JOB_DECLARATION = new BatchSeedJobDeclaration();

  // persistent
  protected String id;
  protected String type;
  protected int size;
  protected int revision;
  protected String seedJobDefinitionId;
  protected String executionJobDefinitionId;

  protected ByteArrayField configuration = new ByteArrayField(this);

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
    seedJobDefinition = new JobDefinitionEntity(BATCH_JOB_DECLARATION);
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
    JobEntity seedJob = BATCH_JOB_DECLARATION.createJobInstance(this);

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

  public void delete(boolean cascadeToHistory) {
    deleteSeedJob();
    getBatchHandler().deleteJobs(this);
    Context.getCommandContext().getBatchManager().delete(this);

    if (cascadeToHistory) {
      Context.getCommandContext().getHistoricJobLogManager().deleteHistoricJobLogsByJobDefinitionId(seedJobDefinitionId);
      Context.getCommandContext().getHistoricJobLogManager().deleteHistoricJobLogsByJobDefinitionId(executionJobDefinitionId);

      Context.getCommandContext().getJobDefinitionManager().delete(getSeedJobDefinition());
      Context.getCommandContext().getJobDefinitionManager().delete(getExecutionJobDefinition());
    }
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

}
