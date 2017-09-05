package com.example.local

import java.io.{DataOutputStream, IOException, ObjectOutputStream, PrintWriter}
import java.net.{ServerSocket, Socket}

import com.example.transport.Sender
import com.google.gson.Gson
import net.sourceforge.jpcap.capture.{PacketCapture, PacketListener}
import net.sourceforge.jpcap.net.{Packet, TCPPacket}

class ScalaSniffer(device: String) {

  val INFINITE = -1
  val PACKET_COUNT = INFINITE

  val FILTER =
    ""

  val pcap = new PacketCapture()
  println("Using device '" + device + "'")
  pcap.open(device, true)
  pcap.setFilter(FILTER, true)
  pcap.addPacketListener(new ScalaPacketHandler)

  println("Capturing packets...")
  pcap.capture(PACKET_COUNT)
}

//object Sender {
//  def send(packet: String): Unit = {
//    try {
//      println(s"Sender has this: $packet")
//      val serverSocket = new ServerSocket(9999)
//      val socket = serverSocket.accept()
//      println("socket created")
//      val out = new ObjectOutputStream(new DataOutputStream(socket.getOutputStream))
//      println("evrythin is created")
//      out.write(packet.getBytes)
//      println("writtrn")
//      out.flush()
//    } catch	 {
//      case e: IOException => e.printStackTrace()
//      case u: UnknownError => u.printStackTrace()
//    }
//    println("and sender send this")
//  }
//}

class ScalaPacketHandler extends PacketListener {

    val serverSocket = new ServerSocket(8585)
    val socket: Socket = serverSocket.accept()
    val out = new PrintWriter(new DataOutputStream(socket.getOutputStream))

  def packetArrived(packet: Packet): Unit = {
    println("packet arrived")
    try {
      packet match {
        case tcpPacket: TCPPacket =>

          //        TODO sending to Spark server
          println("working with tcppacket")
          out.println(tcpPacket.toString)
          out.flush()
          println("flushed")
          println("closed")
        //        Sender.send(tcpPacket.toString)

        /*val data = tcpPacket.getTCPData
			val srcHost = tcpPacket.getSourceAddress
			val dstHost = tcpPacket.getDestinationAddress
			val isoData = new String(data, "ISO-8859-1")
			println(srcHost + " -> " + dstHost + ": " + isoData)*/

        case _ =>
      }
    } catch {
      case e: IOException => e.printStackTrace()
      case ex: Exception => ex.printStackTrace()
    }
  }
}

object ScalaSniffer extends App {

  println(System.getProperty("java.library.path"))
  if (args.length == 1) {
    val sniffer = new ScalaSniffer(args(0))
  } else {
    println("Usage: java Sniffer [device name]")
    println("Available network devices on your machine:")
    val serverSocket = new ServerSocket(8585)
    val socket: Socket = serverSocket.accept()
    val out = new PrintWriter(new DataOutputStream(socket.getOutputStream))
    val devs: Array[String] = PacketCapture.lookupDevices()
    devs.foreach(dev => println("\t" + dev))
    socket.close()
  }
}
