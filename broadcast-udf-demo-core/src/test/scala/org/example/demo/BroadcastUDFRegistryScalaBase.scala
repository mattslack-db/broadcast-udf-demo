package org.example.demo

class BroadcastUDFRegistryScalaBase extends BroadcastUDFRegistryBase[BroadcastUDFRegistryScala] {

  override def createRegistry() =
    new BroadcastUDFRegistryScala()

}