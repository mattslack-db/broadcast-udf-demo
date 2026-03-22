package org.example.demo

class BroadcastUDFRegistryScalaTest extends BroadcastUDFRegistryTest[BroadcastUDFRegistryScala] {

  override def createRegistry() =
    new BroadcastUDFRegistryScala()

}