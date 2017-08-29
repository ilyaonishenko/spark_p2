package com.example.local;


import net.sourceforge.jpcap.capture.PacketCapture;
import net.sourceforge.jpcap.capture.PacketListener;
import net.sourceforge.jpcap.net.Packet;
import net.sourceforge.jpcap.net.TCPPacket;

/**
 * jpcap Tutorial - Sniffer example
 *
 * @author Rex Tsai
 * @version $Revision: 1.1 $
 * @lastModifiedBy $Author: pcharles $
 * @lastModifiedAt $Date: 2002/02/18 21:49:49 $
 */
public class Sniffer
{
    private static final int INFINITE = -1;
    private static final int PACKET_COUNT = INFINITE;
  /*
    private static final String HOST = "203.239.110.20";
    private static final String FILTER =
      "host " + HOST + " and proto TCP and port 23";
  */

    private static final String FILTER =
            // "port 23";
            "";

    public static void main(String[] args) {
//        System.loadLibrary("libjnetcap");
        System.out.println(System.getProperty("java.library.path"));
        try {
            if(args.length == 1){
               Sniffer sniffer = new Sniffer(args[0]);
            } else {
                System.out.println("Usage: java Sniffer [device name]");
                System.out.println("Available network devices on your machine:");
                String[] devs = PacketCapture.lookupDevices();
                for(int i = 0; i < devs.length ; i++)
                    System.out.println("\t" + devs[i]);
            }
        } catch(Exception e) {
            e.printStackTrace();
        }
    }

    public Sniffer(String device) throws Exception {
        // Initialize jpcap
        PacketCapture pcap = new PacketCapture();
        System.out.println("Using device '" + device + "'");
        pcap.open(device, true);
        pcap.setFilter(FILTER, true);
        pcap.addPacketListener(new PacketHandler());

        System.out.println("Capturing packets...");
        pcap.capture(PACKET_COUNT);
    }
}


class PacketHandler implements PacketListener
{
    public void packetArrived(Packet packet) {
        try {
            // only handle TCP packets

            if(packet instanceof TCPPacket) {
                TCPPacket tcpPacket = (TCPPacket)packet;
                byte[] data = tcpPacket.getTCPData();

                String srcHost = tcpPacket.getSourceAddress();
                String dstHost = tcpPacket.getDestinationAddress();
                String isoData = new String(data, "ISO-8859-1");

                System.out.println(srcHost+" -> " + dstHost + ": " + isoData);
            }
        } catch( Exception e ) {
            e.printStackTrace();
        }
    }
}