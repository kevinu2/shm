package main

import (
    "fmt"
    "github.com/kevinu2/shm/ishm"
    "time"
)

type LogHandler struct {

}

func (handler *LogHandler)OnMessage(topic string, eventType string, content []byte){
    fmt.Println(topic)
    fmt.Println(eventType)
    fmt.Println(content)
}

func main(){
    logHandler := new(LogHandler)
    subscriber := new(ishm.ShmSubscriber)
    subscriber.Init(999999, logHandler)
    subscriber.Start()
    for i:=0;i<10;i++{
        time.Sleep(time.Second)
    }
    subscriber.Close()
}
