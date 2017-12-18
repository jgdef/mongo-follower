package com.belmonttech.dev

import groovy.util.logging.Slf4j

@Slf4j
class Shell {

  def static isJenkinsBuild = 'bash -c isJenkinsBuild'.execute().waitFor() == 0

  public static chomp(str) {
    // remove trailing newline if there is one
    def len = str.length()
    if ((len > 0) && (str.getAt(len - 1) == "\n")) {
      return str.substring(0, len - 1)
    } else {
      return str
    }
  }

  public static envContains = { envName, itemName ->
    def value = System.env.get(envName)
    if (value) {
      return value.toLowerCase().tokenize().contains(itemName.toLowerCase())
    } else {
      return false
    }
  }

  def static getBoolean(value) {
    if (value && value.isInteger()) {
      return value.toInteger() as boolean
    } else {
      return false
    }
  }

  public static envGetBoolean(name) {
    def value = System.properties.containsKey(name) ? System.properties.get(name) : System.env.get(name)
    return getBoolean(value)
  }

  public static envGetString(name) {
    def value = System.properties.containsKey(name) ? System.properties.get(name) : System.env.get(name)
    return value;
  }

  public static getShellCommandLine(command, Map options = null) {
    def verbose = options?.containsKey('verbose') ? options['verbose'] : isJenkinsBuild
    def flags = verbose ? '-cex' : '-ce'
    def fullCommand = [System.env.SHELL ?: 'bash', flags, command]
    return fullCommand
  }

  public static execCommandReturningExitValueAndOutput(String[] fullCommand, Map envMap = null) {
    log.info "exec ${fullCommand.join(' ')}"

    Runtime runtime = Runtime.getRuntime()
    def env = envMap ? envMap.collect { k, v -> "${k}=${v}" } as String[] : null
    Process process = runtime.exec(fullCommand, env)

    // wait until input is available or proccess has stopped
    def processStopped = false
    def availableWithInterrupt = { inputStream, sleepMilliseconds = 20 ->
      while (true) {
        if (inputStream.available()) {
          return true
        } else if (processStopped || Thread.interrupted()) {
          return false
        } else {
          try {
            Thread.currentThread().sleep(sleepMilliseconds)
          }
          catch (InterruptedException) {
            processStopped = true
          }
        }
      }
      return false
    }

    // Thread to read and log the stderr of the process
    def errThread = Thread.start {
      InputStream err = process.getErrorStream()
      ByteArrayOutputStream buffer = new ByteArrayOutputStream()
      while (availableWithInterrupt(err)) {
        def result = err.read()
        if (result == -1) {
          break
        } else if ((result == '\n') || (result == '\r')) {
          if (buffer.size() > 0) {
            log.info buffer.toString("UTF-8")
            buffer.reset()
          }
        } else {
          buffer.write(result)
        }
      }
      if (buffer.size() > 0) {
        log.info buffer.toString("UTF-8")
      }
    }

    // Thread to read the process output and save it in a String
    String output
    def outThread = Thread.start {
      InputStream out = process.getInputStream()
      ByteArrayOutputStream buffer = new ByteArrayOutputStream()
      while (availableWithInterrupt(out)) {
        def result = out.read()
        if (result == -1) {
          break
        }
        buffer.write(result)
      }
      output = buffer.toString("UTF-8")
    }

    // Give the reader threads a chance to run.
    Thread.yield()
    sleep 100

    // wait for process to exit then notify reader threads
    process.waitFor()
    errThread.interrupt()
    outThread.interrupt()

    errThread.join()
    outThread.join()

    return [process.exitValue(), output]
  }

  public static shellCommandReturningExitValueAndOutput(command, envMap = null, Map options = null) {
    String[] fullCommand = getShellCommandLine(command, options)
    def (exitValue, output) = execCommandReturningExitValueAndOutput(fullCommand, envMap)
    return [exitValue, chomp(output)]
  }

  public static shellCommand(command, Map envMap = null, Map options = null) {
    def (exitValue, output) = shellCommandReturningExitValueAndOutput(command, envMap, options)
    if (exitValue != 0) {
      throw new Exception("${command} exited with exitValue ${exitValue}")
    }
    return output
  }
}
