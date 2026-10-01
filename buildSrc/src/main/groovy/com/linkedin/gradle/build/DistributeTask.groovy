package com.linkedin.gradle.build

import groovy.json.JsonBuilder
import org.apache.http.HttpResponse
import org.apache.http.client.fluent.Request
import org.apache.http.entity.ContentType
import org.gradle.api.DefaultTask
import org.gradle.api.tasks.TaskAction
import org.jfrog.gradle.plugin.artifactory.dsl.ArtifactoryPluginConvention

/**
 * Publishes a build's artifacts to the configured Artifactory repository.
 */
class DistributeTask extends DefaultTask {

  @TaskAction
  void distributeBuild() {
    ArtifactoryPluginConvention convention = project.convention.plugins.artifactory
    String buildNumber = convention.clientConfig.info.buildNumber
    String buildName = convention.clientConfig.info.buildName
    String context = convention.clientConfig.publisher.contextUrl
    String password = convention.clientConfig.publisher.password

    if (password == null || password == '') {
      throw new IllegalArgumentException('password not set')
    }

    String falseValue = Boolean.FALSE
    String trueValue = Boolean.TRUE
    Map<String, Object> body = [
        'publish'              : trueValue,
        'overrideExistingFiles': falseValue,
        'async'                : trueValue,
        'targetRepo'           : 'maven',
        'sourceRepos'          : ['cruise-control'],
        'dryRun'               : falseValue
    ]

    String bodyString = new JsonBuilder(body)

    String url = "$context/api/build/distribute/$buildName/$buildNumber"
    logger.lifecycle('url {}', url)
    HttpResponse response = Request.Post(url)
        .bodyString(bodyString, ContentType.APPLICATION_JSON)
        .addHeader('X-JFrog-Art-Api', password)
        .execute()
        .returnResponse()

    ByteArrayOutputStream bout = new ByteArrayOutputStream()
    response.entity.writeTo(bout)
    String errMsg = new String(bout.toByteArray())
    logger.lifecycle('Distribute Response: {} {}', response, errMsg)

    if (!Integer.toString(response.statusLine.statusCode).startsWith('2')) {
      throw new IOException('http post failed')
    }
  }

}
