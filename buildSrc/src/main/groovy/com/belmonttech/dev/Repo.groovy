package com.belmonttech.dev

class Repo {
  def static void configure(gradle, repositoryHandler) {

    def artifactoryUsername = ArtifactoryApi.getUsername()
    def artifactoryPassword = ArtifactoryApi.getPassword()
    def repoMain = getMainRepo()
    repositoryHandler.maven {
      url repoMain
      credentials {
        username artifactoryUsername
        password artifactoryPassword
      }
    }
    if (gradle.startParameter.isOffline()) {
      // Look for artifacts cached from either internal or external artifactory repos when running
      // in offline mode.
      def repoAlternate = getAlternateRepo()
      repositoryHandler.maven {
        url repoAlternate
        credentials {
          username artifactoryUsername
          password artifactoryPassword
        }
      }
    }
  }

  def static String getAlternateRepo() {
    return "${ArtifactoryApi.getBaseUrlString(!ArtifactoryApi.getUseExternalRepo())}/remote-repos"
  }

  def static String getMainRepo() {
    return "${ArtifactoryApi.getBaseUrlString()}/remote-repos"
  }
}
