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
package org.camunda.bpm.engine.impl.bpmn.behavior;

import static org.camunda.bpm.engine.impl.util.CallableElementUtil.getCaseDefinitionToCall;

import org.camunda.bpm.engine.impl.cmmn.execution.CmmnCaseInstance;
import org.camunda.bpm.engine.impl.cmmn.model.CmmnCaseDefinition;
import org.camunda.bpm.engine.impl.pvm.delegate.ActivityExecution;
import org.camunda.bpm.engine.runtime.CaseInstance;
import org.camunda.bpm.engine.variable.VariableMap;

/**
 * Implementation to create a new {@link CaseInstance} using the BPMN 2.0 call activity
 *
 * @author Roman Smirnov
 *
 */
public class CaseCallActivityBehavior extends CallableElementActivityBehavior {

  protected void startInstance(ActivityExecution execution, VariableMap variables, String businessKey) {
    CmmnCaseDefinition definition = getCaseDefinitionToCall(execution, getCallableElement());
    CmmnCaseInstance caseInstance = execution.createSubCaseInstance(definition, businessKey);
    caseInstance.create(variables);
  }

}
