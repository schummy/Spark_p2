/**
  * Created by root on 6/7/14.
  */

import java.util

import org.apache.spark.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver
import java.lang.StringBuilder
import org.jnetpcap.protocol.network.Ip4
import org.jnetpcap.protocol.tcpip.Tcp
import org.jnetpcap.{PcapIf, Pcap}
import org.jnetpcap.packet.PcapPacket
import org.jnetpcap.packet.PcapPacketHandler
import java.util.Date
import java.util.ArrayList
import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer


/**
  * Created by root on 5/7/14.
  */


class CustomReceiverJnetPcap(var device:String)
  extends Receiver[(String,(Int, Int))](StorageLevel.MEMORY_AND_DISK_2) with Logging  {
  def main() {
    println("In main")
  }

  def onStart() {
    // Start the thread that receives data over a connection
    if ( ! listNetworkDevices() ) return
    new Thread("PacketReceiver") {
      override def run() {
        receive()
      }
    }.start()
  }
  def receive() {
    val pcap: Pcap = allocatePcap()
    val jpacketHandler = new PcapPacketHandler[String]() {

      var tcp:Tcp = new Tcp(); // Preallocate a Tcp header
      var ip:Ip4 = new Ip4();
      var dIP:Array[Byte] = new Array[Byte](4);
      def nextPacket(packet: PcapPacket, user: String) {
        if (packet.hasHeader(ip)) {
          dIP = packet.getHeader(ip).destination()
         // println(org.jnetpcap.packet.format.FormatUtils.ip(dIP)+"    "+packet.getCaptureHeader.wirelen().toString)
         // store(org.jnetpcap.packet.format.FormatUtils.ip(dIP)+","+packet.getCaptureHeader.wirelen().toString)
          store((org.jnetpcap.packet.format.FormatUtils.ip(dIP), (packet.getCaptureHeader.wirelen(), 1 )))

        }
      }
    }
    pcap.loop(Pcap.LOOP_INFINITE, jpacketHandler, "")
  }

  def allocatePcap(): Pcap = {
    val snaplen = 64 * 1024 // Capture all packets, no trucation
    val flags = Pcap.MODE_PROMISCUOUS // capture all packets
    val timeout = 10 * 1000
    val jsb = new StringBuilder()
    val errbuf = new StringBuilder(jsb)

    val pcap = Pcap.openLive(device, snaplen, flags, timeout, errbuf)
    if (pcap == null) {
      println("Error : " + errbuf.toString())
    }
    pcap
  }

  def listNetworkDevices(): Boolean = {
    val jsb = new StringBuilder()
    val errbuf = new StringBuilder(jsb)

    var alldevs = new util.ArrayList[PcapIf]

    val r: Int = Pcap.findAllDevs(alldevs, errbuf)
    if (r == Pcap.NOT_OK || alldevs.isEmpty()) {
      println(s"Can't read list of devices, error is $errbuf")
      return false
    }
    printNetworkDevices(alldevs)
    true
  }

  def printNetworkDevices(alldevs: util.ArrayList[PcapIf]): Unit = {

    System.out.println("Network devices found:")
    alldevs.foreach(device => {
      val description: String = {
        if (device.getDescription() != null)
          device.getDescription()
        else
          "No description available";
      }

      println(device.getName() + s" [${description}]")
    }
    )
  }

  def onStop() {
  }
}
