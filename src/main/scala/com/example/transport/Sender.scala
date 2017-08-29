package com.example.transport

import java.io.{DataOutputStream, ObjectOutputStream}
import java.net.{InetAddress, Socket}

object Sender {
  def send(packet: String): Unit = {
    println(s"Sender has this: $packet")
    val socket = new Socket(InetAddress.getByName("localhost"), 9999)
    val out = new ObjectOutputStream(new DataOutputStream(socket.getOutputStream))
    out.writeObject(packet)
    out.flush()
    println("and sender send this")
  }
}