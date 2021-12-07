/*
 * Copyright 2019 WeBank
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.webank.wedatasphere.linkis.engineconnplugin.datax.factory

import com.webank.wedatasphere.linkis.common.utils.Logging
import com.webank.wedatasphere.linkis.engineconn.common.creation.EngineCreationContext
import com.webank.wedatasphere.linkis.engineconnplugin.datax.context.DataxEngineConnContext
import com.webank.wedatasphere.linkis.engineconnplugin.datax.util.ClassUtil
import com.webank.wedatasphere.linkis.manager.engineplugin.common.creation.{ExecutorFactory, MultiExecutorEngineConnFactory}
import com.webank.wedatasphere.linkis.manager.label.entity.engine.EngineType
import com.webank.wedatasphere.linkis.manager.label.entity.engine.EngineType.EngineType

class DataxEngineConnFactory extends MultiExecutorEngineConnFactory with Logging{
  override def getExecutorFactories: Array[ExecutorFactory] = executorFactoryArray

  override protected def getDefaultExecutorFactoryClass: Class[_ <: ExecutorFactory] = classOf[DataxExecutorFactory]

  override protected def getEngineConnType: EngineType = EngineType.DATAX

  override protected def createEngineConnSession(engineCreationContext: EngineCreationContext): Any = {
    val sqoopEngineConnContext = new DataxEngineConnContext()
    sqoopEngineConnContext
  }


  private val executorFactoryArray =  Array[ExecutorFactory](ClassUtil.getInstance(classOf[DataxExecutorFactory], new DataxExecutorFactory))
}
