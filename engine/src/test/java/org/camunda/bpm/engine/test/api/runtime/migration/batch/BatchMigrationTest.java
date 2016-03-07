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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.camunda.bpm.engine.ManagementService;
import org.camunda.bpm.engine.RuntimeService;
import org.camunda.bpm.engine.batch.Batch;
import org.camunda.bpm.engine.impl.batch.BatchSeedJobHandler;
import org.camunda.bpm.engine.impl.cfg.ProcessEngineConfigurationImpl;
import org.camunda.bpm.engine.impl.migration.MigrationBatchHandler;
import org.camunda.bpm.engine.impl.util.ClockUtil;
import org.camunda.bpm.engine.management.JobDefinition;
import org.camunda.bpm.engine.migration.MigrationPlan;
import org.camunda.bpm.engine.repository.ProcessDefinition;
import org.camunda.bpm.engine.runtime.Job;
import org.camunda.bpm.engine.test.ProcessEngineRule;
import org.camunda.bpm.engine.test.api.runtime.migration.MigrationTestRule;
import org.camunda.bpm.engine.test.api.runtime.migration.ProcessModels;
import org.joda.time.DateTime;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import groovy.transform.ASTTest;

public class BatchMigrationTest {

  protected ProcessEngineRule rule = new ProcessEngineRule(true);
  protected MigrationTestRule testHelper = new MigrationTestRule(rule);

  @Rule
  public RuleChain ruleChain = RuleChain.outerRule(rule).around(testHelper);

  protected ProcessEngineConfigurationImpl configuration;
  protected RuntimeService runtimeService;
  protected ManagementService managementService;

  protected ProcessDefinition sourceProcessDefinition;
  protected ProcessDefinition targetProcessDefinition;

  protected int defaultNumberOfJobsCreatedBySeedJob;
  protected int defaultNumberOfInvocationPerBatchJob;
  protected int defaultBatchPollTime;

  @Before
  public void initServices() {
    runtimeService = rule.getRuntimeService();
    managementService = rule.getManagementService();
  }

  @Before
  public void saveProcessEngineConfiguration() {
    configuration = (ProcessEngineConfigurationImpl) rule.getProcessEngine().getProcessEngineConfiguration();
    defaultNumberOfJobsCreatedBySeedJob = configuration.getNumberOfJobsCreatedByBatchSeedJob();
    defaultNumberOfInvocationPerBatchJob = configuration.getNumberOfInvocationPerBatchJob();
    defaultBatchPollTime = configuration.getBatchCompletionPollWaitTime();
  }

  @After
  public void removeBatches() {
    for (Batch batch : managementService.createBatchQuery().list()) {
      managementService.deleteBatch(batch.getId(), true);
    }
  }

  @After
  public void resetProcessEngineConfiguration() {
    configuration.setNumberOfJobsCreatedByBatchSeedJob(defaultNumberOfJobsCreatedBySeedJob);
    configuration.setNumberOfInvocationPerBatchJob(defaultNumberOfInvocationPerBatchJob);
    configuration.setBatchCompletionPollWaitTime(defaultBatchPollTime);
  }

  @After
  public void resetClock() {
    ClockUtil.reset();
  }

  @Test
  public void testBatchCreation() {
    // when
    Batch batch = migrateProcessInstancesAsyncForBatchInvocations(1);

    // then a batch is created
    assertNotNull(batch);
    assertNotNull(batch.getId());
    assertEquals("instance-migration", batch.getType());
    assertEquals(10, batch.getSize());
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
    assertEquals(defaultNumberOfJobsCreatedBySeedJob, runtimeService.createProcessInstanceQuery().processDefinitionId(targetProcessDefinition.getId()).count());

    // and the no migration jobs exist
    assertEquals(0, getMigrationJobs().size());

    // but the seed job still exists
    assertNotNull(getSeedJob());
  }

  @Test
  public void testNumberOfJobsCreatedBySeedJobPerInvocation() {
    migrateProcessInstancesAsyncForBatchInvocations(3);

    // when
    executeSeedJob();

    // then the default number of jobs was created
    assertEquals(defaultNumberOfJobsCreatedBySeedJob, getMigrationJobs().size());

    // when the seed job is executed a second time
    executeSeedJob();

    // then the same amount of jobs was created
    assertEquals(2 * defaultNumberOfJobsCreatedBySeedJob, getMigrationJobs().size());

    // when the seed job is executed a third time
    executeSeedJob();

    // then the all jobs where created
    assertEquals(3 * defaultNumberOfJobsCreatedBySeedJob, getMigrationJobs().size());

    // when the seed job is executed again
    executeSeedJob();

    // then no more jobs where created
    assertEquals(3 * defaultNumberOfJobsCreatedBySeedJob, getMigrationJobs().size());
  }

  @Test
  public void testCustomNumberOfJobsCreateBySeedJob() {
    int numberOfJobsCreated = 50;
    int invocationsPerJob = 4;
    ProcessEngineConfigurationImpl configuration = (ProcessEngineConfigurationImpl) rule.getProcessEngine().getProcessEngineConfiguration();
    configuration.setNumberOfJobsCreatedByBatchSeedJob(numberOfJobsCreated);
    configuration.setNumberOfInvocationPerBatchJob(invocationsPerJob);

    migrateProcessInstancesAsyncForBatchInvocations(2);

    // when
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
    int numberOfProcessInstance = numberOfBatchInvocation * configuration.getNumberOfInvocationPerBatchJob() * configuration.getNumberOfJobsCreatedByBatchSeedJob();
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

    return runtimeService.executeMigrationPlanAsync(migrationPlan, processInstanceIds);
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

  protected void executeJob(Job job) {
    managementService.executeJob(job.getId());
  }

}
