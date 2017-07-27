/**
  * Bolt
  * Admin
  *
  * Copyright (c) 2017 Osamu Takahashi
  *
  * This software is released under the MIT License.
  * http://opensource.org/licenses/mit-license.php
  *
  * @author Osamu Takahashi
  */
package com.sopranoworks.bolt

import com.google.cloud.spanner.DatabaseAdminClient

/**
  * Created by takahashi on 2017/05/19.
  */
case class Admin(adminClient:DatabaseAdminClient,instance:String,databaes:String)
