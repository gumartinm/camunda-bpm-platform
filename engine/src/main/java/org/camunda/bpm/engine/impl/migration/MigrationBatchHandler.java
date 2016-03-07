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
package org.camunda.bpm.engine.impl.migration;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.camunda.bpm.engine.ProcessEngineException;
import org.camunda.bpm.engine.impl.batch.BatchEntity;
import org.camunda.bpm.engine.impl.batch.BatchHandler;
import org.camunda.bpm.engine.impl.context.Context;
import org.camunda.bpm.engine.impl.interceptor.CommandContext;
import org.camunda.bpm.engine.impl.jobexecutor.JobDeclaration;
import org.camunda.bpm.engine.impl.migration.MigrationBatchHandler.MigrationBatchConfiguration;
import org.camunda.bpm.engine.impl.persistence.entity.ByteArrayEntity;
import org.camunda.bpm.engine.impl.persistence.entity.ExecutionEntity;
import org.camunda.bpm.engine.impl.persistence.entity.JobDefinitionEntity;
import org.camunda.bpm.engine.impl.persistence.entity.JobEntity;
import org.camunda.bpm.engine.impl.persistence.entity.MessageEntity;
import org.camunda.bpm.engine.impl.util.IoUtil;
import org.camunda.bpm.engine.migration.MigrationPlan;

/**
 * @author Thorben Lindhauer
 *
 */
public class MigrationBatchHandler implements BatchHandler<MigrationBatchConfiguration> {

  public static final String TYPE = "instance-migration";

  public static final MigrationBatchJobDeclaration JOB_DECLARATION = new MigrationBatchJobDeclaration();

  // TODO: serialization JSON

  @Override
  public byte[] writeConfiguration(MigrationBatchConfiguration configuration) {
    // TODO: move to IoUtil? (together with java variable serializer)
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ObjectOutputStream ois = null;
    try {
      ois = new ObjectOutputStream(baos);
      ois.writeObject(configuration);
      return baos.toByteArray();
    }
    catch (Exception e) {
      // TODO: make this proper
      throw new ProcessEngineException("Cannot write batch configuration", e);
    }
    finally {
      IoUtil.closeSilently(ois);
      IoUtil.closeSilently(baos);
    }

  }

  @Override
  public MigrationBatchConfiguration readConfiguration(byte[] serializedConfiguration) {
    ByteArrayInputStream bais = new ByteArrayInputStream(serializedConfiguration);
    ObjectInputStream ois = null;
    try {
      // TODO: use ClassloaderAwareObjectInputStream?
      ois = new ObjectInputStream(bais);
      return (MigrationBatchConfiguration) ois.readObject();
    }
    catch (Exception e) {
      // TODO: make this proper
      throw new ProcessEngineException("Cannot read batch configuration", e);
    }
    finally {
      IoUtil.closeSilently(ois);
      IoUtil.closeSilently(bais);
    }
  }

  @Override
  public boolean createJobs(BatchEntity batch, int numJobsPerSeedInvocation, int numInvocationsPerJobs) {
    MigrationBatchConfiguration configuration = readConfiguration(batch.getConfigurationBytes());
    JobDefinitionEntity jobDefinition = batch.getExecutionJobDefinition();

    List<String> unprocessedProcessInstanceIds = configuration.getProcessInstanceIds();
    List<JobEntity> jobsCreated = new ArrayList<JobEntity>();

    // TODO: make list modification most efficient
    // TODO: don't create jobs with empty list of process instance ids
    while (jobsCreated.size() < numJobsPerSeedInvocation && !unprocessedProcessInstanceIds.isEmpty()) {
      int numInstancesForJob = Math.min(numInvocationsPerJobs, unprocessedProcessInstanceIds.size());

      List<String> idsForJob = new ArrayList<String>();
      for (int i = 0; i < numInstancesForJob; i++) {
        idsForJob.add(unprocessedProcessInstanceIds.remove(0));
      }

      MigrationBatchConfiguration configurationForJob = new MigrationBatchConfiguration();
      configurationForJob.setMigrationPlan(configuration.getMigrationPlan());
      configurationForJob.setProcessInstanceIds(idsForJob);

      ByteArrayEntity configurationEntity = new ByteArrayEntity();
      configurationEntity.setBytes(writeConfiguration(configurationForJob));
      // TODO: setName???
      Context.getCommandContext().getByteArrayManager().insert(configurationEntity);

      MessageEntity jobInstance = JOB_DECLARATION.createJobInstance(configurationEntity);
      jobInstance.setJobDefinition(jobDefinition);
      jobsCreated.add(jobInstance);
      Context.getCommandContext().getJobManager().insert(jobInstance);
    }

    configuration.setProcessInstanceIds(unprocessedProcessInstanceIds);
    writeConfiguration(configuration);

    return unprocessedProcessInstanceIds.isEmpty();
  }

  @Override
  public void deleteJobs(BatchEntity batch) {
    // TODO: this should probably not fetch all the jobs
    // TODO: how do we identify which jobs belong to the given batch?
    // TODO: make sure this uses an index?!
    List<JobEntity> jobs = Context.getCommandContext()
      .getJobManager()
      .findJobsByJobDefinitionId(batch.getExecutionJobDefinitionId());

    for (JobEntity job : jobs) {
      Context.getCommandContext()
        .getByteArrayManager()
        .deleteByteArrayById(job.getJobHandlerConfiguration());

      job.delete();
    }

  }

  @Override
  public String getType() {
    return TYPE;
  }

  @Override
  public JobDeclaration<?, MessageEntity> getJobDeclaration() {
    return JOB_DECLARATION;
  }

  @Override
  public void execute(String configuration, ExecutionEntity execution, CommandContext commandContext, String tenantId) {
    ByteArrayEntity configurationEntity = commandContext.getDbEntityManager().selectById(ByteArrayEntity.class, configuration);

    MigrationBatchConfiguration batchConfiguration = readConfiguration(configurationEntity.getBytes());
    commandContext
      .getProcessEngineConfiguration()
      .getRuntimeService()
      .executeMigrationPlan(batchConfiguration.getMigrationPlan(), batchConfiguration.getProcessInstanceIds());

    commandContext.getByteArrayManager().delete(configurationEntity);
  }

  public static class MigrationBatchConfiguration implements Serializable {

    private static final long serialVersionUID = 1L;

    protected List<String> processInstanceIds;
    protected MigrationPlan migrationPlan;

    public List<String> getProcessInstanceIds() {
      return processInstanceIds;
    }

    public void setProcessInstanceIds(List<String> processInstanceIds) {
      this.processInstanceIds = processInstanceIds;
    }

    public MigrationPlan getMigrationPlan() {
      return migrationPlan;
    }

    public void setMigrationPlan(MigrationPlan migrationPlan) {
      this.migrationPlan = migrationPlan;
    }
  }


}
