package org.example.caso1

object Repository {

  def splitByDelimiter(text: String, delimiter: String): Array[String] = {
    text.split(delimiter)
  }

  def getFirstElementArray(array: Array[String], index: Int): String = {
    array(index).toString
  }

  def getUser(userName: String): String = {
    val userNameInformation = splitByDelimiter(userName, "@")
    val Index = 0
    val user = getFirstElementArray(userNameInformation, Index)
    user
  }

}
