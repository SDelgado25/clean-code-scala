package org.example.caso1

import org.slf4j.LoggerFactory

import scala.util.{Failure, Success, Try}

object CasoPractico1 {

  def main(args: Array[String]): Unit = {
    val logger = LoggerFactory.getLogger(getClass.getSimpleName)
    Try {
      logger.info("Lectura de parametros de ejecucion")
      val username = args(0)
      val password = args(1)

      logger.info("Validacion de usuario")
      if (username.isEmpty) logger.error("Invalid user")
      val user = new User(username, password)
      val existingUser = new ExistingUser()

      logger.info("Valida si el usuario existe")
      if (checkUserExists(user, existingUser)) {
        logger.info("Valido")
        println(s"Bienvenido ${user.user}")
      }
      else {
        logger.error("Invalid user or password")
      }

    } match {
      case Failure(exception) => {
        logger.error("ERROR", exception)
        logger.error(s"ERROR : $exception + ${exception.getCause}")
        throw new Exception(s"ERROR : $exception + ${exception.getCause}", exception)
      }
      case Success(_) => {
        logger.info("PROCESS OK")
      }
    }
  }

  def checkUserExists(user: User, existingUser: ExistingUser): Boolean = {
    user.user == existingUser.user && user.password == existingUser.password
  }


}
