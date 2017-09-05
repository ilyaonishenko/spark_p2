package com.example.local

import java.io.{DataOutputStream, PrintWriter}
import java.net.{ServerSocket, Socket}

import com.example.model.CustomPacket
import net.sourceforge.jpcap.capture.{PacketCapture, PacketListener}
import net.sourceforge.jpcap.net.{Packet, TCPPacket}
import com.github.nscala_time.time.Imports._
import org.joda.time.DateTime

class ScalaSniffer(device: String) {

  val INFINITE: Int = -1
  val PACKET_COUNT: Int = INFINITE

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

class ScalaPacketHandler extends PacketListener {

  val serverSocket = new ServerSocket(8585)
  val socket: Socket = serverSocket.accept()
  val out = new PrintWriter(new DataOutputStream(socket.getOutputStream))

  def packetArrived(packet: Packet): Unit = {
    packet match {
      case tcpPacket: TCPPacket =>
        println("working with tcppacket")
        out.println(
          CustomPacket(tcpPacket.getDestinationAddress,
                       tcpPacket.getData.length,
                       DateTime.now))
        out.flush()
        println("flushed")
      case _ =>
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
