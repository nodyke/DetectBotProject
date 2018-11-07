package com.dbocharov.detect.model

case class Event(
                  unix_time: Long,
                  category_id: Long,
                  ip: String,
                  event: String
                )

case class BotRecord(
                      ip: String,
                      block_date: Long
                    )

