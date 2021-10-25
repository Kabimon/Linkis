/*
 * Copyright 2019 WeBank
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
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

package com.webank.wedatasphere.linkis.engineconn.computation.executor.hook

import com.webank.wedatasphere.linkis.engineconn.callback.hook.CallbackEngineConnHook
import com.webank.wedatasphere.linkis.engineconn.common.creation.EngineCreationContext
import com.webank.wedatasphere.linkis.engineconn.common.engineconn.EngineConn
import com.webank.wedatasphere.linkis.engineconn.core.executor.ExecutorManager
import com.webank.wedatasphere.linkis.manager.common.entity.enumeration.NodeStatus
import com.webank.wedatasphere.linkis.manager.label.entity.engine.EngineConnModeLabel
import scala.collection.JavaConversions._


class ComputationEngineConnHook extends CallbackEngineConnHook {

  override protected def getNodeStatusOfStartSuccess(engineCreationContext: EngineCreationContext,
                                                     engineConn: EngineConn): NodeStatus = NodeStatus.Unlock

  override def afterEngineServerStartSuccess(engineCreationContext: EngineCreationContext,
                                             engineConn: EngineConn): Unit =
    {
      for(label <- engineCreationContext.getLabels()){
          if("engineConnMode".equals(label.getLabelKey) && "once".equals(label.getStringValue)){
            return
          }
      }
      super.afterEngineServerStartSuccess(engineCreationContext, engineConn)
      ExecutorManager.getInstance.getReportExecutor.tryReady()
    }
}
