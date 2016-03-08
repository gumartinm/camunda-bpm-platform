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
package org.camunda.bpm.engine.test.api.runtime.migration.batch;

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import org.camunda.bpm.engine.HistoryService;
import org.camunda.bpm.engine.ManagementService;
import org.camunda.bpm.engine.ProcessEngineException;
import org.camunda.bpm.engine.RuntimeService;
import org.camunda.bpm.engine.batch.Batch;
import org.camunda.bpm.engine.batch.history.HistoricBatch;
import org.camunda.bpm.engine.impl.batch.BatchSeedJobHandler;
import org.camunda.bpm.engine.impl.cfg.ProcessEngineConfigurationImpl;
import org.camunda.bpm.engine.impl.migration.batch.MigrationBatchHandler;
import org.camunda.bpm.engine.impl.util.ClockUtil;
import org.camunda.bpm.engine.management.JobDefinition;
import org.camunda.bpm.engine.migration.MigrationPlan;
import org.camunda.bpm.engine.repository.ProcessDefinition;
import org.camunda.bpm.engine.runtime.Job;
import org.camunda.bpm.engine.test.ProcessEngineRule;
import org.camunda.bpm.engine.test.api.runtime.migration.MigrationTestRule;
import org.camunda.bpm.engine.test.api.runtime.migration.ProcessModels;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

public class BatchMigrationTest {

  protected ProcessEngineRule rule = new ProcessEngineRule(true);
  protected MigrationTestRule testHelper = new MigrationTestRule(rule);

  @Rule
  public RuleChain ruleChain = RuleChain.outerRule(rule).around(testHelper);

  protected ProcessEngineConfigurationImpl configuration;
  protected RuntimeService runtimeService;
  protected ManagementService managementService;
  protected HistoryService historyService;

  protected ProcessDefinition sourceProcessDefinition;
  protected ProcessDefinition targetProcessDefinition;

  protected int defaultNumberOfJobsPerBatchSeedJob;
  protected int defaultNumberOfInvocationPerBatchJob;
  protected int defaultBatchPollTime;

  @Before
  public void initServices() {
    runtimeService = rule.getRuntimeService();
    managementService = rule.getManagementService();
    historyService = rule.getHistoryService();
  }

  @Before
  public void saveProcessEngineConfiguration() {
    configuration = (ProcessEngineConfigurationImpl) rule.getProcessEngine().getProcessEngineConfiguration();
    defaultNumberOfJobsPerBatchSeedJob = configuration.getNumberOfJobsPerBatchSeedJob();
    defaultNumberOfInvocationPerBatchJob = configuration.getNumberOfInvocationPerBatchJob();
    defaultBatchPollTime = configuration.getBatchCompletionPollWaitTime();
  }

  @After
  public void removeBatches() {
    for (Batch batch : managementService.createBatchQuery().list()) {
      managementService.deleteBatch(batch.getId(), true);
    }

    // remove history of completed batches
    for (HistoricBatch historicBatch : historyService.createHistoricBatchQuery().list()) {
      historyService.deleteHistoricBatch(historicBatch.getId());
    }
  }

  @After
  public void resetProcessEngineConfiguration() {
    configuration.setNumberOfJobsPerBatchSeedJob(defaultNumberOfJobsPerBatchSeedJob);
    configuration.setNumberOfInvocationPerBatchJob(defaultNumberOfInvocationPerBatchJob);
    configuration.setBatchCompletionPollWaitTime(defaultBatchPollTime);
  }

  @After
  public void resetClock() {
    ClockUtil.reset();
  }

  @Test
  public void testNullMigrationPlan() {
    try {
      runtimeService.executeMigrationPlan(null).processInstanceIds(Collections.singletonList("process")).executeAsync();
      fail("Should not succeed");
    }
    catch (ProcessEngineException e) {
      assertThat(e.getMessage(), containsString("migration plan is null"));
    }
  }

  @Test
  public void testNullProcessInstanceIds() {
    ProcessDefinition testProcessDefinition = testHelper.deploy(ProcessModels.ONE_TASK_PROCESS);
    MigrationPlan migrationPlan = runtimeService.createMigrationPlan(testProcessDefinition.getId(), testProcessDefinition.getId())
      .mapEqualActivities()
      .build();

    try {
      runtimeService.executeMigrationPlan(migrationPlan).processInstanceIds(null).executeAsync();
      fail("Should not succeed");
    }
    catch (ProcessEngineException e) {
      assertThat(e.getMessage(), containsString("process instance ids is null"));
    }
  }

  @Test
  public void testEmptyProcessInstanceIds() {
    ProcessDefinition testProcessDefinition = testHelper.deploy(ProcessModels.ONE_TASK_PROCESS);
    MigrationPlan migrationPlan = runtimeService.createMigrationPlan(testProcessDefinition.getId(), testProcessDefinition.getId())
      .mapEqualActivities()
      .build();

    try {
      runtimeService.executeMigrationPlan(migrationPlan).processInstanceIds(Collections.<String>emptyList()).executeAsync();
      fail("Should not succeed");
    }
    catch (ProcessEngineException e) {
      assertThat(e.getMessage(), containsString("process instance ids is empty"));
    }
  }

  @Test
  public void testBatchCreation() {
    // when
    Batch batch = migrateProcessInstancesAsyncForBatchInvocations(1);

    // then a batch is created
    assertNotNull(batch);
    assertNotNull(batch.getId());
    assertEquals("instance-migration", batch.getType());
    assertEquals(defaultNumberOfJobsPerBatchSeedJob, batch.getSize());
    assertEquals(defaultNumberOfJobsPerBatchSeedJob, batch.getNumberOfJobsPerSeedJobInvocation());
    assertEquals(defaultNumberOfInvocationPerBatchJob, batch.getNumberOfInvocationsPerJob());
  }

  @Test
  public void testSeedJobCreation() {
    // when
    Batch batch = migrateProcessInstancesAsyncForBatchInvocations(1);

    // then there exists a seed job definition with the batch id as configuration
    JobDefinition seedJobDefinition = getSeedJobDefinition();
    assertNotNull(seedJobDefinition);
    assertEquals(batch.getId(), seedJobDefinition.getJobConfiguration());

    // and there exists a migration job definition
    JobDefinition migrationJobDefinition = getMigrationJobDefinition();
    assertNotNull(migrationJobDefinition);

    // and a seed job with no relation to a process or execution etc.
    Job seedJob = getJobForDefinition(seedJobDefinition);
    assertNotNull(seedJob);
    assertEquals(seedJobDefinition.getId(), seedJob.getJobDefinitionId());
    assertNull(seedJob.getDuedate());
    assertNull(seedJob.getDeploymentId());
    assertNull(seedJob.getProcessDefinitionId());
    assertNull(seedJob.getProcessDefinitionKey());
    assertNull(seedJob.getProcessInstanceId());
    assertNull(seedJob.getExecutionId());

    // but no migration jobs where created
    List<Job> migrationJobs = getJobsForDefinition(migrationJobDefinition);
    assertEquals(0, migrationJobs.size());
  }

  @Test
  public void testMigrationJobsCreation() {
    migrateProcessInstancesAsyncForBatchInvocations(1);
    JobDefinition seedJobDefinition = getSeedJobDefinition();
    JobDefinition migrationJobDefinition = getMigrationJobDefinition();

    // when
    executeSeedJob();

    // then there exist 10 migration jobs
    List<Job> migrationJobs = getJobsForDefinition(migrationJobDefinition);
    assertEquals(10, migrationJobs.size());

    for (Job migrationJob : migrationJobs) {
      assertEquals(migrationJobDefinition.getId(), migrationJob.getJobDefinitionId());
      assertNull(migrationJob.getDuedate());
      assertNull(migrationJob.getDeploymentId());
      assertNull(migrationJob.getProcessDefinitionId());
      assertNull(migrationJob.getProcessDefinitionKey());
      assertNull(migrationJob.getProcessInstanceId());
      assertNull(migrationJob.getExecutionId());
    }

    // and the seed job still exists
    Job seedJob = getJobForDefinition(seedJobDefinition);
    assertNotNull(seedJob);
  }

  @Test
  public void testMigrationJobsExecution() {
    migrateProcessInstancesAsyncForBatchInvocations(1);
    executeSeedJob();
    List<Job> migrationJobs = getMigrationJobs();

    // when
    for (Job migrationJob : migrationJobs) {
      executeJob(migrationJob);
    }

    // then all process instances where migrated
    assertEquals(0, runtimeService.createProcessInstanceQuery().processDefinitionId(sourceProcessDefinition.getId()).count());
    assertEquals(defaultNumberOfJobsPerBatchSeedJob, runtimeService.createProcessInstanceQuery().processDefinitionId(targetProcessDefinition.getId()).count());

    // and the no migration jobs exist
    assertEquals(0, getMigrationJobs().size());

    // but the seed job still exists
    assertNotNull(getSeedJob());
  }

  @Test
  public void testNumberOfJobsCreatedBySeedJobPerInvocation() {
    Batch batch = migrateProcessInstancesAsyncForBatchInvocations(3);

    // when
    executeSeedJob();

    // then the default number of jobs was created
    assertEquals(batch.getNumberOfJobsPerSeedJobInvocation(), getMigrationJobs().size());

    // when the seed job is executed a second time
    executeSeedJob();

    // then the same amount of jobs was created
    assertEquals(2 * batch.getNumberOfJobsPerSeedJobInvocation(), getMigrationJobs().size());

    // when the seed job is executed a third time
    executeSeedJob();

    // then the all jobs where created
    assertEquals(3 * batch.getNumberOfJobsPerSeedJobInvocation(), getMigrationJobs().size());

    // when the seed job is executed again
    executeSeedJob();

    // then no more jobs where created
    assertEquals(3 * batch.getNumberOfJobsPerSeedJobInvocation(), getMigrationJobs().size());
  }

  @Test
  public void testCustomNumberOfJobsCreateBySeedJob() {
    int numberOfJobsCreated = 20;
    int invocationsPerJob = 2;
    ProcessEngineConfigurationImpl configuration = (ProcessEngineConfigurationImpl) rule.getProcessEngine().getProcessEngineConfiguration();
    configuration.setNumberOfJobsPerBatchSeedJob(numberOfJobsCreated);
    configuration.setNumberOfInvocationPerBatchJob(invocationsPerJob);

    // when
    Batch batch = migrateProcessInstancesAsyncForBatchInvocations(2);

    // then the configuration was saved in the batch job
    assertEquals(numberOfJobsCreated, batch.getNumberOfJobsPerSeedJobInvocation());
    assertEquals(invocationsPerJob, batch.getNumberOfInvocationsPerJob());

    // when the seed job is executed
    executeSeedJob();

    // then there exist the first batch of migration jobs
    assertEquals(numberOfJobsCreated, getMigrationJobs().size());

    // when the seed job is executed a second time
    executeSeedJob();

    // then the full batch of migration jobs exist
    assertEquals(2 * numberOfJobsCreated, getMigrationJobs().size());

    // when the seed job is executed a third time
    executeSeedJob();

    // then no new migration jobs are created
    assertEquals(2 * numberOfJobsCreated, getMigrationJobs().size());
  }

  @Test
  public void testSeedJobPollingForCompletion() {
    migrateProcessInstancesAsyncForBatchInvocations(1);

    // when
    Date createDate = new Date(1457326800000L);
    ClockUtil.setCurrentTime(createDate);
    executeSeedJob();

    // then the seed job has a due date of the default batch poll time
    Job seedJob = getSeedJob();
    Date dueDate = new Date(createDate.getTime() + (defaultBatchPollTime * 1000));
    assertEquals(dueDate, seedJob.getDuedate());
  }

  @Test
  public void testSeedJobRemovesBatchAfterCompletion() {
    migrateProcessInstancesAsyncForBatchInvocations(1);
    executeSeedJob();
    executeMigrationJobs();

    // when
    executeSeedJob();

    // then the batch was completed and removed
    assertEquals(0, managementService.createBatchQuery().count());

    // and the seed jobs was removed
    assertEquals(0, managementService.createJobQuery().count());
  }

  @Test
  public void testBatchDeletion() {
    Batch batch = migrateProcessInstancesAsync(10);
    executeSeedJob();

    // when
    managementService.deleteBatch(batch.getId(), true);

    // then the batch was deleted
    assertEquals(0, managementService.createBatchQuery().count());

    // and the seed and migration job definition were deleted
    assertEquals(0, managementService.createJobDefinitionQuery().count());

    // and the seed job and migration jobs were deleted
    assertEquals(0, managementService.createJobQuery().count());
  }

  // helper //////////////////////

  protected Batch migrateProcessInstancesAsyncForBatchInvocations(int numberOfBatchInvocation) {
    int numberOfProcessInstance = numberOfBatchInvocation * configuration.getNumberOfInvocationPerBatchJob() * configuration.getNumberOfJobsPerBatchSeedJob();
    return migrateProcessInstancesAsync(numberOfProcessInstance);
  }

  protected Batch migrateProcessInstancesAsync(int numberOfProcessInstances) {
    sourceProcessDefinition = testHelper.deploy(ProcessModels.ONE_TASK_PROCESS);
    targetProcessDefinition = testHelper.deploy(ProcessModels.ONE_TASK_PROCESS);

    List<String> processInstanceIds = new ArrayList<String>(numberOfProcessInstances);
    for (int i = 0; i < numberOfProcessInstances; i++) {
      processInstanceIds.add(
        runtimeService.startProcessInstanceById(sourceProcessDefinition.getId()).getId());
    }

    MigrationPlan migrationPlan = runtimeService
      .createMigrationPlan(sourceProcessDefinition.getId(), targetProcessDefinition.getId())
      .mapEqualActivities()
      .build();

    return runtimeService.executeMigrationPlan(migrationPlan).processInstanceIds(processInstanceIds).executeAsync();
  }

  protected JobDefinition getSeedJobDefinition() {
    return managementService.createJobDefinitionQuery().jobType(BatchSeedJobHandler.TYPE).singleResult();
  }

  protected JobDefinition getMigrationJobDefinition() {
    return managementService.createJobDefinitionQuery().jobType(MigrationBatchHandler.TYPE).singleResult();
  }

  protected Job getJobForDefinition(JobDefinition jobDefinition) {
    return managementService.createJobQuery().jobDefinitionId(jobDefinition.getId()).singleResult();
  }

  protected List<Job> getJobsForDefinition(JobDefinition jobDefinition) {
    return managementService.createJobQuery().jobDefinitionId(jobDefinition.getId()).list();
  }


  protected Job getSeedJob() {
    return getJobForDefinition(getSeedJobDefinition());
  }

  protected List<Job> getMigrationJobs() {
    return getJobsForDefinition(getMigrationJobDefinition());
  }

  protected void executeSeedJob() {
    executeJob(getSeedJob());
  }

  protected void executeMigrationJobs() {
    for (Job migrationJob : getMigrationJobs()) {
      executeJob(migrationJob);
    }
  }

  protected void executeJob(Job job) {
    managementService.executeJob(job.getId());
  }

}
