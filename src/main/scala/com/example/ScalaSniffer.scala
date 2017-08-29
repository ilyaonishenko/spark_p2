package com.example

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

class ScalaPacketHandler extends PacketListener {

  def packetArrived(packet: Packet): Unit = {
    // only handle TCP packets

    packet match {
      case tcpPacket: TCPPacket =>
        val data = tcpPacket.getTCPData

        val srcHost = tcpPacket.getSourceAddress
        val dstHost = tcpPacket.getDestinationAddress
        val isoData = new String(data, "ISO-8859-1")

        println(srcHost + " -> " + dstHost + ": " + isoData)
      case _ => println("Not TCP packet!")
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
    val devs: Array[String] = PacketCapture.lookupDevices()
    devs.foreach(dev => println("\t" + dev))
  }
}
