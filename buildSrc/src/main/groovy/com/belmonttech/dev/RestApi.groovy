package com.belmonttech.dev

import groovy.json.JsonOutput
import groovy.util.logging.Slf4j

@Slf4j
class RestApi {

  def username
  def password

  def baseUrlString

  def deleteAbsolute(urlString) {
    log.info('delete ' + urlString)

    def connection = getConnection(urlString)
    connection.requestMethod = 'DELETE'
    def code = connection.responseCode
    def content
    log.info(Integer.toString(code))
    if (code != 204) {
      content = connection.errorStream.text
      log.info(content)
    }
    return [code, content]
  }

  def deleteRelative(relativeUrlString) {
    def urlString = baseUrlString + relativeUrlString
    return deleteAbsolute(urlString)
  }

  def getRelative(relativeUrlString) {
    def urlString = baseUrlString + relativeUrlString
    log.info('get ' + urlString)

    def connection = getConnection(urlString)
    def code = connection.responseCode
    def content
    try {
      content = connection.content.text
    } catch (IOException e) {
      content = connection.errorStream.text
    }
    log.info(Integer.toString(code))
    log.info(content)

    return [code, content]
  }

  def headRelative(relativeUrlString) {
    def urlString = baseUrlString + relativeUrlString
    log.info('head ' + urlString)

    def connection = getConnection(urlString)
    connection.requestMethod = 'HEAD'
    def code = connection.responseCode
    def message = connection.responseMessage
    log.info(Integer.toString(code))
    log.info(message)

    return [code:code, message:message, modifed:connection.lastModified, length:connection.contentLength, type:connection.contentType, urlString:urlString]
  }

  def postAbsolute(urlString, requestProperties, data) {
    log.info('post ' + urlString)

    def connection = getConnection(urlString)
    connection.requestMethod = 'POST'
    connection.doOutput = true;
    requestProperties.each { k, v -> connection.setRequestProperty(k, v) }
    connection.outputStream.withWriter { writer ->
      writer << data
    }
    def code = connection.responseCode
    def content = connection.content.text
    log.info(Integer.toString(code))
    log.info(content)

    return [code, content]
  }

  def postAbsoluteAsJson(urlString, data) {
    def json = JsonOutput.toJson(data)
    postAbsolute(urlString, ['Content-type': 'application/json'], json)
  }

  def postRelative(relativeUrlString, requestProperties, data) {
    def urlString = baseUrlString + relativeUrlString
    postAbsolute(urlString, requestProperties, data)
  }

  def postRelativeAsText(relativeUrlString, data) {
    def urlString = baseUrlString + relativeUrlString
    postAbsolute(urlString, ['Content-type': 'text/plain'], data)
  }

  def getAuthString() {
    def credentials = "${username}:${password}"
    def authString = 'Basic ' + credentials.getBytes( 'ISO-8859-1').encodeBase64().toString()
    return authString
  }

  def getConnection(urlString) {
    def url = urlString.toURL()
    def connection = url.openConnection()
    if (username && password) {
      connection.setRequestProperty('Authorization', getAuthString())
    }
    return connection
  }
}
