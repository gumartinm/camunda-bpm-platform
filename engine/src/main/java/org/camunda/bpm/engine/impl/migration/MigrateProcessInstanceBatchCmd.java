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


import java.util.List;

import org.camunda.bpm.engine.batch.Batch;
import org.camunda.bpm.engine.impl.batch.BatchEntity;
import org.camunda.bpm.engine.impl.interceptor.Command;
import org.camunda.bpm.engine.impl.interceptor.CommandContext;
import org.camunda.bpm.engine.impl.migration.MigrationBatchHandler.MigrationBatchConfiguration;
import org.camunda.bpm.engine.impl.persistence.entity.JobEntity;
import org.camunda.bpm.engine.migration.MigrationPlan;

/**
 * @author Thorben Lindhauer
 */
public class MigrateProcessInstanceBatchCmd implements Command<Batch> {

  protected static final MigrationBatchHandler BATCH_HANDLER = new MigrationBatchHandler();

  protected List<String> processInstanceIds;
  protected MigrationPlan migrationPlan;

  public MigrateProcessInstanceBatchCmd(MigrationPlan migrationPlan, List<String> processInstanceIds) {
    this.processInstanceIds = processInstanceIds;
    this.migrationPlan = migrationPlan;
  }

  @Override
  public Batch execute(CommandContext commandContext) {
    // Create batch
    //  1. persist configuration (i.e. process instance ids)
    //  2. create seed job that knows where to look for configuration

    BatchEntity batch = new BatchEntity();
    batch.setType(MigrationBatchHandler.TYPE);
    batch.setSize(processInstanceIds.size());

    MigrationBatchConfiguration configuration = new MigrationBatchConfiguration();
    configuration.setMigrationPlan(migrationPlan);
    configuration.setProcessInstanceIds(processInstanceIds);
    batch.setConfigurationBytes(BATCH_HANDLER.writeConfiguration(configuration));

    commandContext.getBatchManager().insert(batch);

    JobEntity seedJob = batch.createSeedJob();
    commandContext.getJobManager().insert(seedJob);

    return batch;
  }

}
