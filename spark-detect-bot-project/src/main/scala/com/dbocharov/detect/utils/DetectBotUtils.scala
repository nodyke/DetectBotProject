package com.dbocharov.detect.utils

import com.dbocharov.detect.model.Event

object DetectBotUtils {

  def checkHighDifferenceBot(iter: Iterable[Event],max_second:Int):Boolean = {
    val list = iter.toList.sortBy(event => event.unix_time)
    var previous_event:Option[Event] = Option.empty[Event]
    var currentEvent:Event = null
    var check_no_view = true
    val iterator = list.iterator
    while (iterator.hasNext)
    {
      currentEvent = iterator.next()
      currentEvent.event match {
        case "click"  =>
          previous_event = Option.apply[Event](currentEvent)
        case "view" =>
          if(previous_event.isDefined)
            if(currentEvent.unix_time - previous_event.get.unix_time > max_second*50) //Cause botgen can't generate high difference, value were random selected
                return true
            else check_no_view = false
      }
    }
    check_no_view
  }
}
