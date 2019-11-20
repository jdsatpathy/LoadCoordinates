package com.validation

import com.typesafe.config.ConfigFactory

trait GenericValidation {
  //val envProp = ConfigFactory.load().getConfig(args(0))
  def intValidate: Boolean = {
    true
  }

  def lengthValidate(length: Int): Boolean = {
    true
  }

}
