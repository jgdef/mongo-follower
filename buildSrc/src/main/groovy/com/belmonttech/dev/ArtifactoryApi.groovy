package com.belmonttech.dev

import groovy.json.JsonSlurper

class ArtifactoryApi extends RestApi {

  public ArtifactoryApi() {
    super()
    init(getUseExternalRepo())
  }

  public ArtifactoryApi(Map args) {
    super()
    init(args.externalRepo)
  }

  def init(boolean useExternalRepo) {
    username = getUsername()
    password = getPassword()

    baseUrlString = getBaseUrlString(useExternalRepo) + '/'
  }

  def static getUsername() {
    return Shell.chomp(['bash', '-c', 'getKeychainItem artifactory-username'].execute().text)
  }

  def static getPassword() {
    return Shell.chomp(['bash', '-c', 'getKeychainItem artifactory-password'].execute().text)
  }

  def static String getBaseUrlString(boolean useExternalRepo) {
    return Shell.chomp(['bash', '-c', "getRepoUrl ${useExternalRepo ? '1' : '0'}"].execute().text)
  }

  def static String getBaseUrlString() {
    return getBaseUrlString(getUseExternalRepo())
  }

  def static getUseExternalRepo() {
    return Shell.envGetBoolean('EXTERNAL_REPO')
  }

  // dependencyMap can contains these keys and their values: group, name, version, ext, classifier
  def headArtifact(dependencyMap) {
    def group      = dependencyMap.group
    def artifact   = dependencyMap.name
    def version    = dependencyMap.version
    def extension  = dependencyMap.ext
    def classifier = dependencyMap.classifier

    def groupPath = group.replaceAll('\\.', '/')
    def relativeUrl = classifier
    ? "simple/libs-release-local/${groupPath}/${artifact}/${version}/${artifact}-${version}-${classifier}.${extension}"
    : "simple/libs-release-local/${groupPath}/${artifact}/${version}/${artifact}-${version}.${extension}"
    return headRelative(relativeUrl)
  }

  def getCreated(args) {
    def repos = args.repos ?: 'libs-release-local'
    def from  = args.from  ?: 0
    def to    = args.to    ?: (new Date()).time
    def (code, content) = getRelative("api/search/creation?from=${from}&to=${to}&repos=${repos}")
    if ((code != 200) && (code != 404)) {
      throw new Exception('getCreated: could not search artifacts')
    }
    return new JsonSlurper().parseText(content)
  }

  def deleteItemAbsolute(uri) {
    def (code, content) =  deleteAbsolute(uri)
    if (code != 204) {
      throw new Exception("deleteItemAbsolute ${code} ${content}")
    }
  }

  def deleteItemRelative(uri) {
    def (code, content) =  deleteRelative(uri)
    if (code != 204) {
      throw new Exception("deleteItemRelative ${code} ${content}")
    }
  }

  def query(aql) {
    def (code, content) = postRelativeAsText('api/search/aql', aql)
    if (code != 200) {
      throw new Exception("aql query failed ${code} ${content}")
    }
    return new JsonSlurper().parseText(content)
  }

  def getInfo(repo, path, name) {
    def (code, content) = getRelative("api/storage/${repo}/${path}/${name}")
    if (code != 200) {
      throw new Exception("info query failed ${code} ${content}")
    }
    return new JsonSlurper().parseText(content)
  }
}
