package org.example.caso1

class User(_username: String,
           _password: String) {
  def username: String = _username

  def password: String = _password

  def user: String = {
    Repository.getUser(_username)
  }

}
