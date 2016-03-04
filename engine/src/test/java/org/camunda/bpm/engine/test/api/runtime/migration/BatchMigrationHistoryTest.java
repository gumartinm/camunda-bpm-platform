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
import org.camunda.bpm.engine.batch.history.HistoricBatch;
import org.camunda.bpm.engine.migration.MigrationPlan;
import org.camunda.bpm.engine.repository.ProcessDefinition;
import org.camunda.bpm.engine.test.ProcessEngineRule;
import org.junit.After;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

/**
 * @author Thorben Lindhauer
 *
 */
public class BatchMigrationHistoryTest {

  protected ProcessEngineRule rule = new ProcessEngineRule(true);
  protected MigrationTestRule testHelper = new MigrationTestRule(rule);

  protected Batch batch;

  @Rule
  public RuleChain ruleChain = RuleChain.outerRule(rule).around(testHelper);

  @After
  public void tearDown() {
    if (batch != null) {
      rule.getManagementService().deleteBatch(batch.getId(), true);
    }
  }

  @Test
  public void testHistoricBatchCreation() {
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

    batch = rule.getRuntimeService().executeMigrationPlanAsync(migrationPlan, processInstanceIds);

    // when
    HistoricBatch historicBatch = rule.getHistoryService().createHistoricBatchQuery().singleResult();

    Assert.assertNotNull(historicBatch);
    Assert.assertEquals(batch.getId(), historicBatch.getId());
    Assert.assertEquals(batch.getType(), historicBatch.getType());
    Assert.assertEquals(batch.getSize(), historicBatch.getSize());

    // TODO: assert start and end time

  }

  // TODO: test
  // * deletion
  // * end time when batch finishes
  // * should it be possible to retrieve the configuration from history?
  // * should the job definitions be referenced from the historic entity?
  // * should table have a revision?
}
