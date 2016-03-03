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
package org.camunda.bpm.engine.test.api.runtime.migration;

import java.util.ArrayList;
import java.util.List;

import org.camunda.bpm.engine.batch.Batch;
import org.camunda.bpm.engine.migration.MigrationPlan;
import org.camunda.bpm.engine.repository.ProcessDefinition;
import org.camunda.bpm.engine.runtime.Job;
import org.camunda.bpm.engine.test.ProcessEngineRule;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

/**
 * @author Thorben Lindhauer
 *
 */
public class BatchMigrationTest {

  protected ProcessEngineRule rule = new ProcessEngineRule(true);
  protected MigrationTestRule testHelper = new MigrationTestRule(rule);

  @Rule
  public RuleChain ruleChain = RuleChain.outerRule(rule).around(testHelper);

  @Test
  public void testBatchCreation() {
    ProcessDefinition sourceProcessDefinition = testHelper.deploy(ProcessModels.ONE_TASK_PROCESS);
    ProcessDefinition targetProcessDefinition = testHelper.deploy(ProcessModels.ONE_TASK_PROCESS);

    List<String> processInstanceIds = new ArrayList<String>();
    for (int i = 0; i < 10; i++) {
      processInstanceIds.add(
          rule.getRuntimeService().startProcessInstanceById(sourceProcessDefinition.getId()).getId());
    }

    MigrationPlan migrationPlan = rule.getRuntimeService()
        .createMigrationPlan(sourceProcessDefinition.getId(), targetProcessDefinition.getId())
        .mapEqualActivities()
        .build();

    // when
    Batch batch = rule.getRuntimeService().executeMigrationPlanAsync(migrationPlan, processInstanceIds);

    // then
    Assert.assertNotNull(batch);
    Assert.assertNotNull(batch.getId());
    Assert.assertEquals("instance-migration", batch.getType());
    Assert.assertEquals(10, batch.getSize());
  }

  @Test
  public void testSeedJobCreation() {

    ProcessDefinition sourceProcessDefinition = testHelper.deploy(ProcessModels.ONE_TASK_PROCESS);
    ProcessDefinition targetProcessDefinition = testHelper.deploy(ProcessModels.ONE_TASK_PROCESS);

    List<String> processInstanceIds = new ArrayList<String>();
    for (int i = 0; i < 10; i++) {
      processInstanceIds.add(
          rule.getRuntimeService().startProcessInstanceById(sourceProcessDefinition.getId()).getId());
    }

    MigrationPlan migrationPlan = rule.getRuntimeService()
        .createMigrationPlan(sourceProcessDefinition.getId(), targetProcessDefinition.getId())
        .mapEqualActivities()
        .build();

    // when
    rule.getRuntimeService().executeMigrationPlanAsync(migrationPlan, processInstanceIds);

    // then there is a seed job
    Job seedJob = rule.getManagementService().createJobQuery().singleResult();
    Assert.assertNotNull(seedJob);

    // TODO: assert configuration?



  }

  @Test
  public void testMigrationJobsCreation() {

    ProcessDefinition sourceProcessDefinition = testHelper.deploy(ProcessModels.ONE_TASK_PROCESS);
    ProcessDefinition targetProcessDefinition = testHelper.deploy(ProcessModels.ONE_TASK_PROCESS);

    List<String> processInstanceIds = new ArrayList<String>();
    for (int i = 0; i < 10; i++) {
      processInstanceIds.add(
          rule.getRuntimeService().startProcessInstanceById(sourceProcessDefinition.getId()).getId());
    }

    MigrationPlan migrationPlan = rule.getRuntimeService()
        .createMigrationPlan(sourceProcessDefinition.getId(), targetProcessDefinition.getId())
        .mapEqualActivities()
        .build();

    // when
    rule.getRuntimeService().executeMigrationPlanAsync(migrationPlan, processInstanceIds);
    Job seedJob = rule.getManagementService().createJobQuery().singleResult();
    rule.getManagementService().executeJob(seedJob.getId());

    // then are 10 jobs to migrate the instances
    List<Job> jobs = rule.getManagementService().createJobQuery().list();
    Assert.assertEquals(10, jobs.size());
    // TODO: assert configuration?

  }

  @Test
  public void testMigrationJobsExecution() {

    ProcessDefinition sourceProcessDefinition = testHelper.deploy(ProcessModels.ONE_TASK_PROCESS);
    ProcessDefinition targetProcessDefinition = testHelper.deploy(ProcessModels.ONE_TASK_PROCESS);

    List<String> processInstanceIds = new ArrayList<String>();
    for (int i = 0; i < 10; i++) {
      processInstanceIds.add(
          rule.getRuntimeService().startProcessInstanceById(sourceProcessDefinition.getId()).getId());
    }

    MigrationPlan migrationPlan = rule.getRuntimeService()
        .createMigrationPlan(sourceProcessDefinition.getId(), targetProcessDefinition.getId())
        .mapEqualActivities()
        .build();

    // when
    rule.getRuntimeService().executeMigrationPlanAsync(migrationPlan, processInstanceIds);
    Job seedJob = rule.getManagementService().createJobQuery().singleResult();
    rule.getManagementService().executeJob(seedJob.getId());
    List<Job> jobs = rule.getManagementService().createJobQuery().list();

    // then are 10 jobs to migrate the instances

    for (Job job : jobs) {
      rule.getManagementService().executeJob(job.getId());
    }
    Assert.assertEquals(10, rule.getRuntimeService()
        .createProcessInstanceQuery().processDefinitionId(targetProcessDefinition.getId()).count());
  }

  @Test
  public void testSeedJobRecreation() {

  }
}
