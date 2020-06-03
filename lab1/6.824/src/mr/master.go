package mr

import (
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"


type Master struct {
	// Your definitions here.
	mapStore maptasks;
	mapFinished mapFinished
	reduceFiles reduceFiles
	reduceStore reducetasks;
	reduceFinished reduceFinished
}
type maptasks struct {
	tasks map[string]int64;
	sync.Mutex
}
type reducetasks struct {
	tasks map[string]int64;
	sync.Mutex
}
type reduceFiles struct {
	tasks map[string][]string;
	sync.Mutex
}
type mapFinished struct {
	mapFinish bool;
	sync.Mutex
}
type reduceFinished struct {
	reduceFinish bool;
	sync.Mutex
}
var mapMutex sync.Mutex
var mapTask=maptasks{make(map[string]int64),mapMutex}
var reduceMutex sync.Mutex
var reduceTask=reducetasks{make(map[string]int64),reduceMutex}
var reduceFileMutex sync.Mutex
var reduceFile=reduceFiles{make(map[string][]string),reduceFileMutex}
var mapFinishMutex sync.Mutex
var mapFinish=mapFinished{false,mapFinishMutex}
var reduceFinishMutex sync.Mutex
var reduceFinish=reduceFinished{false,reduceFinishMutex}
// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (m *Master) WorkAssign(args *WorkRequest, reply *WorkIdReply)error{
	m.mapFinished.Lock()
	if m.mapFinished.mapFinish {
		//assign ReduceWork
		m.mapFinished.Unlock()
		m.reduceFinished.Lock()
		if m.reduceFinished.reduceFinish {
			m.reduceFinished.Unlock()
			reply.WorkType=-2
			return nil;
		}
		m.reduceFinished.Unlock()
		m.reduceStore.Lock()
		for k,v:=range m.reduceStore.tasks{
			if v==0 {
				reply.FileName=m.reduceFiles.tasks[k];
				reply.WorkType=2;
				reply.WorkID=k;
				m.reduceStore.tasks[k]=time.Now().Unix();
				m.reduceStore.Unlock()
				return nil
			}
		}
		m.reduceStore.Unlock()
		reply.WorkType=-1
		return nil
	}
	//assign Mapwork
	m.mapStore.Lock()
	m.mapFinished.Unlock()
	for k,v:=range m.mapStore.tasks{
		if v==0 {
			reply.FileName=append(reply.FileName,k)

			reply.WorkType=1;
			m.mapStore.tasks[k]=time.Now().Unix();
			m.mapStore.Unlock()
			return nil
		}
	}
	m.mapStore.Unlock()
	reply.WorkType=-1
	return nil
}




func (m *Master) ReceiveMapFinish(args *FinishSignal, reply *FinishReply)error{
	m.reduceStore.Lock()
	for k,v:=range args.ReduceFiles{
		m.reduceStore.tasks[k]=0
		m.reduceFiles.tasks[k]=append(m.reduceFiles.tasks[k],v)
	}
	m.reduceStore.Unlock()

	m.mapStore.Lock()

	m.mapStore.tasks[args.FileName]=-1;
	m.mapFinished.Lock()

	m.mapFinished.mapFinish=true

	for _,v:=range m.mapStore.tasks{
		if v>=0 {
			m.mapFinished.mapFinish=false
			break
		}
	}
	m.mapFinished.Unlock()
	m.mapStore.Unlock()
	reply.Rep=1;

	return nil
}
func (m *Master) ReceiveReduceFinish(args *ReduceFinishSig, reply *ExampleReply)error{
	m.reduceStore.Lock()
	m.reduceStore.tasks[args.FileNumber]=-1;
	m.reduceFinished.Lock()
	m.reduceFinished.reduceFinish=true
	for _,v:=range m.reduceStore.tasks{
		if v>=0 {
			m.reduceFinished.reduceFinish=false
			break
		}
	}
	m.reduceFinished.Unlock()
	m.reduceStore.Unlock()
	return nil
}
//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	m.reduceFinished.Lock()
	tmp:=m.reduceFinished.reduceFinish
	m.reduceFinished.Unlock()
	return tmp
}
func (m *Master) checkAlive(ch chan string) error {
	for {
		time.Sleep(100*time.Millisecond)
		if m.reduceFinished.reduceFinish {
			close(ch)
			break;
		}
		if !m.mapFinished.mapFinish {
			m.mapStore.Lock()
			for k, v := range m.mapStore.tasks {
				if v>0&&(time.Now().Unix()-v)>=10 {
					ch<-"find death"
					m.mapStore.tasks[k]=0;
				}
			}
			m.mapStore.Unlock()
		}

		if !m.reduceFinished.reduceFinish {
			m.reduceStore.Lock()
			for k, v := range m.reduceStore.tasks {
				if v>0&&(time.Now().Unix()-v)>=10	 {
					ch<-"find death"
					m.reduceStore.tasks[k]=0;
				}
			}
			m.reduceStore.Unlock()
		}
	}
	return nil
}
//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{mapTask,mapFinish,reduceFile,reduceTask,reduceFinish}
	for _,mapT := range files{
		m.mapStore.tasks[mapT]=0;
	}
	ch:=make(chan string,10)

	go m.checkAlive(ch)


	m.server()
	return &m
}
