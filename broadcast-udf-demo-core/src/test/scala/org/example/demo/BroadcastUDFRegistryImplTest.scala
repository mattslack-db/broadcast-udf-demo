package org.example.demo

class BroadcastUDFRegistryImplTest extends BroadcastUDFRegistryTest[BroadcastUDFRegistryImpl] {

  override def createRegistry() =
    new BroadcastUDFRegistryImpl()

}
