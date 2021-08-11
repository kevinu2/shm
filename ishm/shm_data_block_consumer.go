package ishm

import (
	"errors"
	"fmt"
	"log"
	"time"
	"unsafe"
)

type TLVCallBack func(*TagTLV)

type ShmConsumerStatus int32

const (
	ShmConsumerOk      ShmConsumerStatus = 0
	ShmConsumerReadErr ShmConsumerStatus = 1
	ShmConsumerLenErr  ShmConsumerStatus = 2
	ShmConsumerInitErr ShmConsumerStatus = 3
	ShmConsumerNoData  ShmConsumerStatus = 4
	ShmConsumerUnAttached ShmConsumerStatus = 5
)

//after//shmi, err := shmdata.GetShareMemoryInfo(999999)

type ShmDataBlockConsumer struct {
	PreTag        uint64
	CurTag        uint64
	TopicLen      uint64
	MaxContentLen uint64
	MaxShmSize    uint64
	CurOffset     uint64
	PreOffset     uint64
	SegLen        uint64
	ShmKey        int64
	IsRunning     bool
	sm            *Segment
	readCnt       uint64
	handler ConsumerHandler
	cmdChannel  chan ShmCommand
	stopChannel chan bool
}

func (consumer *ShmDataBlockConsumer) Init(key int64, maxSHMSize uint64, maxContentLen uint64, handler ConsumerHandler) (bool, error) {
	consumer.ShmKey = key
	/*if !HasKeyofSHM(key) {
		return false
	}*/
	sm, err := CreateWithKey(key, 0)
	if err != nil {
		fmt.Printf("Init consume err key-%v\n\n", key)
		return false, err
	}
	consumer.MaxShmSize = maxSHMSize
	consumer.MaxContentLen = maxContentLen
	consumer.SegLen = maxContentLen + 80
	consumer.CurOffset = 16
	consumer.sm = sm
	consumer.IsRunning = false
	consumer.handler = handler
	consumer.stopChannel = make(chan bool, 32)
	return true, nil
}

func (consumer *ShmDataBlockConsumer) GetCmdChannel() chan ShmCommand {
	return consumer.cmdChannel
}

func (consumer *ShmDataBlockConsumer) Reset() {
	consumer.CurOffset = 16
	consumer.PreTag = 0
	consumer.CurTag = 0
	consumer.IsRunning = false
}

func (consumer *ShmDataBlockConsumer) Start() bool{
	var noDataCnt int32
	noDataCnt = 0
	go func() {
		for{
			select{
			case <-consumer.stopChannel:
				consumer.sm.Detach()
				return
			default:

			}
			tlv,status := consumer.Next()
			switch status {
			case ShmConsumerOk:
				if consumer.handler != nil{
					topic := string(tlv.Topic[:tlv.TopicLen])
					eventType := string(tlv.EventType[:tlv.EventTypeLen])
					consumer.handler.OnMessage(topic, eventType, tlv.Value[:tlv.Len])
				}
				noDataCnt = 0
			case ShmConsumerReadErr, ShmConsumerLenErr:
				consumer.Reset()
			case ShmConsumerNoData:
				noDataCnt += 1
				if noDataCnt%1000 == 0 {
					time.Sleep(time.Millisecond)
					noDataCnt = 0
				}
			default:

			}
		}
	}()
	return true
}

func (consumer *ShmDataBlockConsumer) Close(){
	consumer.stopChannel <- true
}

func (consumer *ShmDataBlockConsumer) Next() (*TagTLV, ShmConsumerStatus) {
	//tl := shmdata.TagTL{}
	//od, err := consumer.sm.ReadChunk(int64(unsafe.Sizeof(shmdata.TagTL)), int64(consumer.CurOffset))
	if consumer.sm == nil {
		return nil, ShmConsumerInitErr
	}
	if consumer.sm.IsAttached(){
		if consumer.readCnt >= 10000{
			consumer.sm.Detach()
			_,err := consumer.sm.Attach()
			if err != nil{
				fmt.Println(err)
				return nil, ShmConsumerUnAttached
			}
			consumer.readCnt = 0
		}
	}else{
		_,err := consumer.sm.Attach()
		if err != nil{
			fmt.Println(err)
			return nil, ShmConsumerUnAttached
		}
		consumer.readCnt = 0
	}
	consumer.readCnt += 1
	od, err := consumer.sm.ReadChunkWithoutAttach(16, int64(consumer.CurOffset))
	if err != nil {
		return nil, ShmConsumerReadErr
	}
	data := *(*[]byte)(unsafe.Pointer(&od))
	var tll *TagTL = *(**TagTL)(unsafe.Pointer(&data))
	if tll.Len > consumer.MaxContentLen {
		return nil, ShmConsumerLenErr
	}
	if tll.Len > 0 {
		if (tll.Tag > consumer.PreTag) || (tll.Tag == 0 && consumer.PreTag == 18446744073709551615 || consumer.PreTag == 0) {
			//copySize := int64(unsafe.Sizeof(tl)) + int64(64) + int64(tll.Len)
			copySize := int64(16) + int64(64) + int64(tll.Len)
			od, err = consumer.sm.ReadChunkWithoutAttach(copySize, int64(consumer.CurOffset))
			consumer.PreTag = tll.Tag
			consumer.PreOffset = consumer.CurOffset
			consumer.CurOffset += consumer.SegLen
			if consumer.CurOffset+consumer.SegLen > consumer.MaxShmSize {
				fmt.Printf("Shm data block-%v new cycle\n", consumer.ShmKey)
				consumer.CurOffset = 16
			}
			consumer.IsRunning = true
			data = *(*[]byte)(unsafe.Pointer(&od))
			//readtlv = *(**shmdata.TagTLV)(unsafe.Pointer(&data))
			return *(**TagTLV)(unsafe.Pointer(&data)), ShmConsumerOk
		}
	} else {
		if consumer.CurOffset != 16 {
			//od, err := consumer.sm.ReadChunk(int64(unsafe.Sizeof(tl)), int64(consumer.CurOffset))
			od, err := consumer.sm.ReadChunkWithoutAttach(int64(16), int64(consumer.CurOffset))
			if err != nil {
				//log.Fatal(err)
				return nil, ShmConsumerReadErr
			}
			data := *(*[]byte)(unsafe.Pointer(&od))
			var headTll *TagTL = *(**TagTL)(unsafe.Pointer(&data))
			if headTll.Len > 0 && (headTll.Tag > consumer.PreTag || (headTll.Tag == 0 && consumer.PreTag == 18446744073709551615)) {
				//new cycle
				consumer.PreOffset = consumer.CurOffset
				fmt.Printf("Worker-%v new cycle headtag-%v\n", consumer.ShmKey, headTll.Tag)
				//copySize := int64(unsafe.Sizeof(tl)) + int64(64) + int64(headTll.Len)
				copySize := int64(16) + int64(64) + int64(headTll.Len)
				od, err = consumer.sm.ReadChunkWithoutAttach(copySize, 16)
				consumer.PreTag = headTll.Tag
				consumer.CurOffset = consumer.SegLen + 16
				consumer.IsRunning = true
				data = *(*[]byte)(unsafe.Pointer(&od))
				//readtlv = *(**shmdata.TagTLV)(unsafe.Pointer(&data))
				return *(**TagTLV)(unsafe.Pointer(&data)), ShmConsumerOk
			}
		}
	}
	return nil, ShmConsumerNoData
}

func (consumer *ShmDataBlockConsumer) Detach() error{
	if consumer.sm == nil {
		return errors.New("hello")
	}
	return consumer.sm.Detach()
}

func (consumer *ShmDataBlockConsumer) AutoNext() (*TagTLV, ShmConsumerStatus) {
	//tl := shmdata.TagTL{}
	//od, err := consumer.sm.ReadChunk(int64(unsafe.Sizeof(shmdata.TagTL)), int64(consumer.CurOffset))
	//var tlv* TagTLV = nil
	if consumer.sm == nil {
		return nil, ShmConsumerInitErr
	}

	dd :=make([]byte,sizeOfTagTLVStruct)
	pos,err:=consumer.sm.Read(dd)
	if err != nil{

	}
	log.Println(pos)

	tlv:=BytesToTagTLVStruct(dd)

	if tlv.Len > consumer.MaxContentLen {
		return tlv, ShmConsumerLenErr
	}

	if tlv.Len > 0 {
		if uint64(tlv.Tag) > consumer.PreTag || tlv.Tag == 0 || consumer.PreTag == 18446744073709551615 || consumer.PreTag == 0 {

			consumer.PreTag = uint64(tlv.Tag)
			consumer.PreOffset = consumer.CurOffset
			consumer.CurOffset += consumer.SegLen
			if consumer.CurOffset+consumer.SegLen > consumer.MaxShmSize {
				fmt.Printf("Worker-%v new cycle\n", consumer.ShmKey)
				consumer.CurOffset = 16
			}
			consumer.IsRunning = true
			return tlv,ShmConsumerOk

		}
	} else {
		if consumer.CurOffset != 16 {
			if tlv.Len > 0 &&
				(uint64(tlv.Tag) > consumer.PreTag || (tlv.Tag == 0 && consumer.PreTag == 18446744073709551615)) {

				consumer.PreOffset = consumer.CurOffset
				fmt.Printf("Worker-%v new cycle headtag-%v\n", consumer.ShmKey, tlv.Tag)

				consumer.PreTag = uint64(tlv.Tag)
				consumer.CurOffset = consumer.SegLen + 16
				consumer.IsRunning = true

				return tlv, ShmConsumerOk
			}
		}
	}
	return nil, ShmConsumerNoData
}

func StartSubscribe(key int64, callBack TLVCallBack) bool {
	shmi, err := GetShareMemoryInfo(key,false)
	if err != nil {
		fmt.Printf("Get Config memory err: %v\n", err)
		return false
	}
	for i := 0; uint64(i) < shmi.Count; i++ {
		go func() {
			consumer := ShmDataBlockConsumer{}
			success,_ := consumer.Init(int64(shmi.Key[i]), shmi.MaxSHMSize, shmi.MaxContentLen, nil)
			if success{
				var noDataCnt int32
				noDataCnt = 0
				for {
					tlv, status := consumer.Next()
					switch status {
					case ShmConsumerOk:
						callBack(tlv)
						noDataCnt = 0
					case ShmConsumerReadErr:
						consumer.Reset()
					case ShmConsumerLenErr:
						consumer.Reset()
					case ShmConsumerInitErr:
						consumer.Init(int64(shmi.Key[i]), shmi.MaxSHMSize, shmi.MaxContentLen,nil)
					case ShmConsumerNoData:
						noDataCnt += 1
						if noDataCnt%1000 == 0 {
							time.Sleep(time.Millisecond)
							noDataCnt = 0
						}
					default:

					}
				}
			}
		}()
	}
	return true
}
