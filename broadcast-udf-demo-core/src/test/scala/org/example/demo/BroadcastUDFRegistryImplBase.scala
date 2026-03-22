package org.example.demo

class BroadcastUDFRegistryImplBase extends BroadcastUDFRegistryBase[BroadcastUDFRegistryImpl] {

  override def createRegistry() =
    new BroadcastUDFRegistryImpl()

}