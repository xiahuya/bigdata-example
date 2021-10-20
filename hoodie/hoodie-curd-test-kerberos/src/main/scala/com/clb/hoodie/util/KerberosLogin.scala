package com.clb.hoodie.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.UserGroupInformation

/**
  *
  * @author Xiahu
  * @create 2020/7/16
  */
class KerberosLogin {
  def kerberosLogin(): Unit = {
    System.setProperty("java.security.krb5.conf", "E:\\conf\\nvwa\\local\\krb5.conf")
    val config = new Configuration
    UserGroupInformation.setConfiguration(config)
    UserGroupInformation.loginUserFromKeytab("hive/master@HADOOP.COM", "E:\\conf\\\\nvwa\\local\\hive.keytab")
  }
}

object KerberosLogin {

}
