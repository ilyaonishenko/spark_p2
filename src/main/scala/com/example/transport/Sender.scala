package com.example.transport

import java.io.{IOException, _}
import java.net.{InetAddress, ServerSocket, Socket}

object Sender {
	def send(packet: String): Unit = {
		try {
			println(s"Sender has this: $packet")
			val serverSocket = new ServerSocket(10016)
			val socket = serverSocket.accept()
			println("socket created")
			val out = new ObjectOutputStream(new DataOutputStream(socket.getOutputStream))
			println("evrythin is created")
			out.write(packet.getBytes)
			println("writtrn")
			out.flush()
		} catch	 {
			case e: IOException => e.printStackTrace()
			case u: UnknownError => u.printStackTrace()
		}
		println("and sender send this")
	}
}