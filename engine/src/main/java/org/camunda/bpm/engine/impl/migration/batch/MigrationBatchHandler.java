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
package org.camunda.bpm.engine.impl.migration.batch;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.camunda.bpm.engine.ProcessEngineException;
import org.camunda.bpm.engine.impl.batch.BatchEntity;
import org.camunda.bpm.engine.impl.batch.BatchHandler;
import org.camunda.bpm.engine.impl.context.Context;
import org.camunda.bpm.engine.impl.interceptor.CommandContext;
import org.camunda.bpm.engine.impl.jobexecutor.JobDeclaration;
import org.camunda.bpm.engine.impl.persistence.entity.ByteArrayEntity;
import org.camunda.bpm.engine.impl.persistence.entity.ByteArrayManager;
import org.camunda.bpm.engine.impl.persistence.entity.ExecutionEntity;
import org.camunda.bpm.engine.impl.persistence.entity.JobDefinitionEntity;
import org.camunda.bpm.engine.impl.persistence.entity.JobEntity;
import org.camunda.bpm.engine.impl.persistence.entity.JobManager;
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

  public void handle(BatchEntity batch) {
    MigrationBatchConfiguration configuration = readConfiguration(batch.getConfigurationBytes());
    if (!configuration.getProcessInstanceIds().isEmpty()) {
      // create further jobs
      boolean done = createJobs(batch, configuration);
      if (!done) {
        batch.createSeedJob();
      }
      else {
        batch.createMonitorJob();
      }
    }
    else {
      // check if batch was finished
      boolean done = checkJobs(batch);
      if (!done) {
        batch.createMonitorJob();
      }
      else {
        batch.delete(false);
      }
    }
  }

  public boolean createJobs(BatchEntity batch, MigrationBatchConfiguration configuration) {
    CommandContext commandContext = Context.getCommandContext();
    ByteArrayManager byteArrayManager = commandContext.getByteArrayManager();
    JobManager jobManager = commandContext.getJobManager();

    JobDefinitionEntity jobDefinition = batch.getExecutionJobDefinition();

    List<String> processInstanceIds = configuration.getProcessInstanceIds();
    MigrationPlan migrationPlan = configuration.getMigrationPlan();
    int numberOfJobsPerSeedJobInvocation = batch.getNumberOfJobsPerSeedJobInvocation();
    int numberOfInvocationsPerJob = batch.getNumberOfInvocationsPerJob();

    int numberOfInstancesToProcess = Math.min(numberOfInvocationsPerJob * numberOfJobsPerSeedJobInvocation, processInstanceIds.size());
    List<String> processInstancesToProcess = processInstanceIds.subList(0, numberOfInstancesToProcess);

    int numberOfJobsToCreate = Math.min(
      numberOfJobsPerSeedJobInvocation,
      (int) Math.ceil(processInstanceIds.size() / numberOfInvocationsPerJob)
    );

    // TODO: make list modification most efficient
    // TODO: don't create jobs with empty list of process instance ids
    for (int i = 0; i < numberOfJobsToCreate; i++) {
      int lastJobId = Math.min(numberOfInvocationsPerJob, processInstancesToProcess.size());
      List<String> idsForJob = processInstancesToProcess.subList(0, lastJobId);

      MigrationBatchConfiguration jobConfiguration = new MigrationBatchConfiguration();
      jobConfiguration.setMigrationPlan(migrationPlan);
      jobConfiguration.setProcessInstanceIds(new ArrayList<String>(idsForJob));

      ByteArrayEntity configurationEntity = new ByteArrayEntity();
      configurationEntity.setBytes(writeConfiguration(jobConfiguration));
      // TODO: setName???
      byteArrayManager.insert(configurationEntity);

      MessageEntity jobInstance = JOB_DECLARATION.createJobInstance(configurationEntity);
      jobInstance.setJobDefinition(jobDefinition);
      jobManager.insert(jobInstance);

      idsForJob.clear();
    }

    batch.setConfigurationBytes(writeConfiguration(configuration));

    return processInstanceIds.isEmpty();
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

  protected boolean checkJobs(BatchEntity batch) {
    List<JobEntity> batchJobs = Context.getCommandContext()
      .getJobManager()
      .findJobsByJobDefinitionId(batch.getExecutionJobDefinitionId());

    return batchJobs.isEmpty();
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
      .executeMigrationPlan(batchConfiguration.getMigrationPlan())
        .processInstanceIds(batchConfiguration.getProcessInstanceIds())
        .execute();

    commandContext.getByteArrayManager().delete(configurationEntity);
  }

}
