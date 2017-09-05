package com.example.model

import com.github.nscala_time.time.Imports._
import org.joda.time.DateTime


case class CustomPacket(destAdrr: String, size: Int, time: DateTime)
