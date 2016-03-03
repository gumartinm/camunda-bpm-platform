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

import org.camunda.bpm.engine.batch.Batch;
import org.camunda.bpm.engine.impl.db.DbEntity;
import org.camunda.bpm.engine.impl.persistence.entity.JobEntity;
import org.camunda.bpm.engine.impl.persistence.entity.Nameable;
import org.camunda.bpm.engine.impl.persistence.entity.util.ByteArrayField;

/**
 * @author Thorben Lindhauer
 *
 */
public class BatchEntity implements Batch, DbEntity, Nameable {

  public static final BatchSeedJobDeclaration BATCH_JOB_DECLARATION = new BatchSeedJobDeclaration();

  // persistent
  protected String id;
  protected String type;

  // TODO: implement
  protected int size;

  protected ByteArrayField configuration = new ByteArrayField(this);

  // transient
  protected BatchHandler<?> handler;

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

  @Override
  public String getName() {
    return getId();
  }

  public void setConfiguration(byte[] configuration) {
    this.configuration.setByteArrayValue(configuration);
  }

  public byte[] getConfiguration() {
    return this.configuration.getByteArrayValue();
  }

  @Override
  public Object getPersistentState() {
    HashMap<String, Object> persistentState = new HashMap<String, Object>();


    return persistentState;
  }

  public JobEntity createSeedJob() {
    return BATCH_JOB_DECLARATION.createJobInstance(this);
  }

}
