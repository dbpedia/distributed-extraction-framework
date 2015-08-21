package org.dbpedia.extraction.util

import com.jcraft.jsch.{JSch, JSchException, ChannelExec, Session}
import java.io.IOException
import scala.io.Source
/**
 * Utility trait for creating an SSH session and executing remote commands.
 */
trait RemoteExecute
{
  val jsch = new JSch()

  def addIdentity(privateKeyPath: String, passphrase: String) = jsch.addIdentity(privateKeyPath, passphrase)

  def addIdentity(privateKeyPath: String) = jsch.addIdentity(privateKeyPath)

  //val password = Source.fromFile("/home/gonephishing/dbpedia-extraction/passwd").getLines.mkString

  def createSession(userName: String, host: String): Session =
  {
    println("User name: "+userName)

    val session = jsch.getSession(userName, host)
    //session.setPassword(password)
    session.setConfig("UserKnownHostsFile", "/dev/null")
    session.setConfig("CheckHostIP", "no")
    session.setConfig("StrictHostKeyChecking", "no")


    session.connect()
    println("Connected")
    session
  }

  def execute(session: Session, command: String): String =
  {
    val outputBuffer = new StringBuilder()

    val channel = session.openChannel("exec").asInstanceOf[ChannelExec]
    channel.setCommand(command)
    channel.connect()
    channel.setErrStream(System.err)

    val commandOutput = channel.getInputStream
    var readByte = commandOutput.read()

    while (readByte != 0xffffffff)
    {
      outputBuffer.append(readByte)
      readByte = commandOutput.read()
    }

    channel.disconnect()
    outputBuffer.toString()
  }
}
