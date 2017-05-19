package com.sopranoworks.bolt

import com.google.cloud.spanner.DatabaseAdminClient

/**
  * Created by takahashi on 2017/05/19.
  */
case class Admin(adminClient:DatabaseAdminClient,instance:String,databaes:String)
