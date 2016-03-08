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


import static org.camunda.bpm.engine.impl.util.EnsureUtil.ensureNotEmpty;
import static org.camunda.bpm.engine.impl.util.EnsureUtil.ensureNotNull;

import java.util.List;

import org.camunda.bpm.engine.BadUserRequestException;
import org.camunda.bpm.engine.batch.Batch;
import org.camunda.bpm.engine.impl.ProcessEngineLogger;
import org.camunda.bpm.engine.impl.batch.BatchEntity;
import org.camunda.bpm.engine.impl.batch.BatchHandler;
import org.camunda.bpm.engine.impl.cfg.ProcessEngineConfigurationImpl;
import org.camunda.bpm.engine.impl.interceptor.Command;
import org.camunda.bpm.engine.impl.interceptor.CommandContext;
import org.camunda.bpm.engine.impl.migration.MigrationLogger;
import org.camunda.bpm.engine.impl.migration.MigrationPlanExecutionBuilderImpl;
import org.camunda.bpm.engine.migration.MigrationPlan;

/**
 * @author Thorben Lindhauer
 */
public class MigrateProcessInstanceBatchCmd implements Command<Batch> {

  protected static final MigrationLogger LOGGER = ProcessEngineLogger.MIGRATION_LOGGER;

  protected MigrationPlanExecutionBuilderImpl migrationPlanExecutionBuilder;

  public MigrateProcessInstanceBatchCmd(MigrationPlanExecutionBuilderImpl migrationPlanExecutionBuilder) {
    this.migrationPlanExecutionBuilder = migrationPlanExecutionBuilder;
  }

  @Override
  @SuppressWarnings("unchecked")
  public Batch execute(CommandContext commandContext) {
    ProcessEngineConfigurationImpl engineConfiguration = commandContext.getProcessEngineConfiguration();
    // FIXME: will this burn in flames with custom handlers?
    BatchHandler<MigrationBatchConfiguration> batchHandler = (BatchHandler<MigrationBatchConfiguration>) engineConfiguration.getBatchHandler(MigrationBatchHandler.TYPE);

    MigrationBatchConfiguration configuration = createConfiguration();
    BatchEntity batch = createBatch(batchHandler, configuration, engineConfiguration.getNumberOfJobsPerBatchSeedJob(), engineConfiguration.getNumberOfInvocationPerBatchJob());

    commandContext.getBatchManager().insert(batch);
    batch.createSeedJobDefinition();
    batch.createExecutionJobDefinition();
    batch.fireHistoricStartEvent();

    batch.createSeedJob();

    return batch;
  }

  protected MigrationBatchConfiguration createConfiguration() {
    MigrationPlan migrationPlan = migrationPlanExecutionBuilder.getMigrationPlan();
    List<String> processInstanceIds = migrationPlanExecutionBuilder.getProcessInstanceIds();

    ensureNotNull(BadUserRequestException.class, "Migration plan cannot be null", "migration plan", migrationPlan);
    ensureNotEmpty(BadUserRequestException.class, "Process instance ids cannot be null or empty", "process instance ids", processInstanceIds);

    MigrationBatchConfiguration configuration = new MigrationBatchConfiguration();
    configuration.setMigrationPlan(migrationPlan);
    configuration.setProcessInstanceIds(processInstanceIds);
    return configuration;
  }

  protected BatchEntity createBatch(BatchHandler<MigrationBatchConfiguration> batchHandler, MigrationBatchConfiguration configuration, int numberOfJobsPerSeedJobInvocation, int numberOfInvocationPerBatchJob) {
    BatchEntity batch = new BatchEntity();
    batch.setType(batchHandler.getType());
    batch.setSize(configuration.getProcessInstanceIds().size());
    batch.setNumberOfJobsPerSeedJobInvocation(numberOfJobsPerSeedJobInvocation);
    batch.setNumberOfInvocationsPerJob(numberOfInvocationPerBatchJob);
    batch.setConfigurationBytes(batchHandler.writeConfiguration(configuration));
    return batch;
  }

}
