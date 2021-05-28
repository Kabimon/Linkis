package com.webank.wedatasphere.linkis.datasource.client.request

import com.webank.wedatasphere.linkis.datasource.client.exception.DataSourceClientBuilderException
import com.webank.wedatasphere.linkis.httpclient.request.GetAction

class MetadataGetTablePropsAction extends GetAction with DataSourceAction {
  private var dataSourceId: Long = _
  private var database: String = _
  private var table: String = _

  override def suffixURLs: Array[String] = Array("metadata", "props", dataSourceId.toString, "db", database, "table", table)
}

object MetadataGetTablePropsAction {
  def builder(): Builder = new Builder

  class Builder private[MetadataGetTablePropsAction]() {
    private var dataSourceId: Long = _
    private var database: String = _
    private var table: String = _
    private var system:String = _
    private var user: String = _

    def setUser(user: String): Builder = {
      this.user = user
      this
    }

    def setDataSourceId(dataSourceId: Long): Builder = {
      this.dataSourceId = dataSourceId
      this
    }

    def setDatabase(database: String): Builder = {
      this.database = database
      this
    }

    def setTable(table: String): Builder = {
      this.table = table
      this
    }

    def setSystem(system: String): Builder = {
      this.system = system
      this
    }

    def build(): MetadataGetTablePropsAction = {
      if(dataSourceId == null) throw new DataSourceClientBuilderException("dataSourceId is needed!")
      if(database == null) throw new DataSourceClientBuilderException("database is needed!")
      if(table == null) throw new DataSourceClientBuilderException("table is needed!")
      if(system == null) throw new DataSourceClientBuilderException("system is needed!")
      if(user == null) throw new DataSourceClientBuilderException("user is needed!")

      val metadataGetTablePropsAction = new MetadataGetTablePropsAction
      metadataGetTablePropsAction.dataSourceId = this.dataSourceId
      metadataGetTablePropsAction.database = this.database
      metadataGetTablePropsAction.table = this.table
      metadataGetTablePropsAction.setParameter("system", system)
      metadataGetTablePropsAction.setUser(user)
      metadataGetTablePropsAction
    }
  }

}
