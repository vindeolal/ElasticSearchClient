package com.vkd.elasticsearch.client


import java.net.InetAddress

import org.apache.http.HttpHost

import org.elasticsearch.action.bulk.BulkRequestBuilder
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.{RestClient, RestHighLevelClient}
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.TransportAddress
import org.elasticsearch.common.xcontent.XContentType
import org.elasticsearch.transport.client.PreBuiltTransportClient

import org.slf4j.LoggerFactory


object IndexClient {

  val logger = LoggerFactory.getLogger("IndexClient")
  var batch = 0 //to count the number of batches

  //Transport Client API using bulk POST
  def transportClient(indexName: String, indexTypeName: String, id: String, jsonString: List[String]) = {

    batch += 1
    val setting = Settings.builder().put("cluster.name", "elasticsearch").build()
    val client = new PreBuiltTransportClient(setting)
    client.addTransportAddress(new TransportAddress(InetAddress.getByName("localhost"), 9300))
    val bulkRequest: BulkRequestBuilder = client.prepareBulk()

    jsonString.foreach(request => {
      bulkRequest.add(client.prepareIndex(indexName, indexTypeName, id)
        .setSource(request, XContentType.JSON))
    })

    val bulkResponse = bulkRequest.execute().actionGet
    if (bulkResponse.hasFailures) {
      logger.error(s"Request failed for some records in batch $batch")
      bulkResponse.getItems.foreach(x => {
        if (x.isFailed) {
          logger.error(s"Failed Id : ${x.getId}")
        }
      })
      logger.error(bulkResponse.buildFailureMessage())
    }
    else {
      logger.info(s"Request successful for ${jsonString.length} records in batch $batch")
    }
    client.close()
  }


  //Using High level Rest Client API
  def highLevelRestClient(index: String, typ: String, id: String, jsonString: String) = {
    val client = new RestHighLevelClient(
      RestClient.builder(new HttpHost("localhost", 9200, "http")))

    val request = new IndexRequest(index, typ, id)

    request.source(jsonString, XContentType.JSON)
    val response = client.index(request)

    logger.info(response.getResult.getLowercase)

    client.close()
  }

}
