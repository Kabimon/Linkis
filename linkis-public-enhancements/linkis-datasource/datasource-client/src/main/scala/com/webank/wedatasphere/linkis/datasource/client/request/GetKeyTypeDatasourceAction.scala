package com.webank.wedatasphere.linkis.datasource.client.request

import com.webank.wedatasphere.linkis.datasource.client.config.DatasourceClientConfig.DATA_SOURCE_SERVICE_MODULE
import com.webank.wedatasphere.linkis.datasource.client.exception.DataSourceClientBuilderException
import com.webank.wedatasphere.linkis.httpclient.request.GetAction

class GetKeyTypeDatasourceAction extends GetAction with DataSourceAction{
  private var dataSourceTypeId: Long = _

  override def suffixURLs: Array[String] = Array(DATA_SOURCE_SERVICE_MODULE.getValue, "key_define","type", dataSourceTypeId.toString)

  private var user:String = _

  override def setUser(user: String): Unit = this.user = user

  override def getUser: String = this.user
}
object GetKeyTypeDatasourceAction{
  def builder(): Builder = new Builder

  class Builder private[GetKeyTypeDatasourceAction]() {
    private var dataSourceTypeId: Long = _
    private var user: String = _

    def setUser(user: String): Builder = {
      this.user = user
      this
    }

    def setDataSourceTypeId(dataSourceTypeId: Long): Builder = {
      this.dataSourceTypeId = dataSourceTypeId
      this
    }

    def build(): GetKeyTypeDatasourceAction = {
      if(user == null) throw new DataSourceClientBuilderException("user is needed!")

      val getKeyTypeDatasourceAction = new GetKeyTypeDatasourceAction
      getKeyTypeDatasourceAction.dataSourceTypeId = this.dataSourceTypeId
      getKeyTypeDatasourceAction.setUser(user)
      getKeyTypeDatasourceAction
    }
  }
}

