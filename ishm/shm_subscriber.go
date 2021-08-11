package ishm

import (
    "fmt"
    "time"
)

type ConsumerHandler interface {
    OnMessage(topic string, eventType string, content []byte)
}

type ShmCmdEnum int32

const(
    ShmCmdOK ShmCmdEnum = 0
    ShmCmdUnAttach ShmCmdEnum = 1
    ShmCmdStop ShmCmdEnum = 2
)

type ShmCommand struct {
    cmd ShmCmdEnum
    shmid int64
}

type ShmSubscriber struct {
    confShmId int64
    handler ConsumerHandler
    consumerList []*ShmDataBlockConsumer
    cmdChannel chan ShmCommand
}

func (subscriber *ShmSubscriber)Init(confShmId int64, handler ConsumerHandler){
    subscriber.confShmId = confShmId
    subscriber.handler = handler
    subscriber.cmdChannel = make(chan ShmCommand, 128)
}

func (subscriber *ShmSubscriber)Start(){
    go func() {
        stop := false
        for{
            if stop{
                break
            }
            needRestart := false
            shmi, err := GetShareMemoryInfo(subscriber.confShmId, false)
            if err == nil{
                fmt.Printf("Get config shm %v success\n", subscriber.confShmId)
                for i := uint64(0); i <= shmi.Count; i++ {
                    curKey := shmi.Key[i]
                    consumer := new(ShmDataBlockConsumer)
                    _,err := consumer.Init(int64(curKey), shmi.MaxSHMSize, shmi.MaxContentLen, subscriber.handler)
                    if err == nil{
                        fmt.Printf("Get data shm %v success\n", curKey)
                        consumer.Start()
                    }else{
                        needRestart = true
                        break
                    }
                    subscriber.consumerList = append(subscriber.consumerList, consumer)
                }
            }else{
                fmt.Println(err)
                needRestart = true
            }

            for{
                select {
                case ShmCmd := <- subscriber.cmdChannel:
                    if ShmCmd.cmd == ShmCmdStop{
                        stop = true
                    }
                default:

                }
                for _,c := range subscriber.consumerList{
                    select{
                    case shmCmd := <- c.GetCmdChannel():
                        if shmCmd.cmd == ShmCmdUnAttach{
                            fmt.Println("Data shm attach err, need re attach")
                            needRestart = true
                        }
                    default:

                    }
                }
                if needRestart || stop{
                    break
                }
                time.Sleep(time.Millisecond * 10)
            }
            for _,c := range subscriber.consumerList{
                c.Close()
            }
            subscriber.consumerList = subscriber.consumerList[:0]
        }
    }()
}

func (subscriber *ShmSubscriber)Close(){
    subscriber.cmdChannel <- ShmCommand{ShmCmdStop, 0}
}