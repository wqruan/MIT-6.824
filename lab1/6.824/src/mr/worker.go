package mr

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"


//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}
type ByKey []KeyValue
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }
//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// Your worker implementation here.
	for  {

		time.Sleep(5*time.Millisecond)

		workRep:=requestWork()

		if workRep.WorkType==-2 {
			break
		}
		if  workRep.WorkType==-1{
			continue
		}
		//Execute map task
		if workRep.WorkType==1 {
			filename:=workRep.FileName[0]
			file, err := os.Open(filename)
			if err != nil {
				log.Fatalf("cannot open %v", filename)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", filename)
			}
			file.Close()
			kva := mapf(filename, string(content))
			sort.Sort(ByKey(kva))
			reduceFiles :=make(map[string]string);
			index:=0
			taskN:=strconv.Itoa(ihash(filename))
			i:=0
			var encoderStore []*json.Encoder
			for i<15{
				var buffer bytes.Buffer
				buffer.WriteString("./mr-")
				buffer.WriteString(taskN)
				buffer.WriteString("-")
				buffer.WriteString(strconv.Itoa(i))
				name:=buffer.String()
				reduceFiles[strconv.Itoa(i)]=name
				ofile, _ := os.OpenFile(name,os.O_RDWR|os.O_CREATE|os.O_APPEND,0644)
				enc:=json.NewEncoder(ofile)
				encoderStore=append(encoderStore,enc)
				i++
			}
			for index<len(kva) {
				kv:=kva[index]
				reduceId:=ihash(kv.Key)%15
				encoderStore[reduceId].Encode(&kv)
				index++;
			}
			finishMap(reduceFiles,filename)
		}
		//execute reduce task
		if workRep.WorkType==2 {

			var buffer bytes.Buffer
			inputfiles:=workRep.FileName
			workID:=workRep.WorkID
			buffer.WriteString("mr-out-")
			buffer.WriteString(workRep.WorkID)
			name:=buffer.String()
			ofile, _ := os.Create(name)
			var kva []KeyValue
			for _,filename:=range inputfiles{
				file, err := os.Open(filename)
				if err != nil {
					log.Fatalf("cannot open %v", filename)
				}
				dec:=json.NewDecoder(file)
					for {
						var kv KeyValue
						if err := dec.Decode(&kv); err != nil {
							break
						}
						kva= append(kva, kv)
					}
				file.Close()

			}
			sort.Sort(ByKey(kva))
			i:=0
			for i < len(kva) {
				j := i + 1
				for j < len(kva) && kva[j].Key == kva[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, kva[k].Value)
				}
				output := reducef(kva[i].Key, values)
				fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

				i = j
			}
			for _,tmp:=range inputfiles{
				os.Remove(tmp)
			}
			ofile.Close()
			finishReduce(workID)
		}
	}
	// uncomment to send the Example RPC to the master.
	// CallExample()

}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}
func requestWork()WorkIdReply{

	workReq:=WorkRequest{1}
	workRep:=WorkIdReply{}
	call("Master.WorkAssign",&workReq,&workRep)
	return workRep
}


func finishMap(reduceFiles map[string]string,fileName string){
	finishSignal:=FinishSignal{fileName,reduceFiles }
	finishRep:=FinishReply{}
	call("Master.ReceiveMapFinish",&finishSignal,&finishRep)
}
func finishReduce(reduceTask string){
	finishSignal:=ReduceFinishSig{reduceTask}
	finishRep:=ExampleReply{}
	call("Master.ReceiveReduceFinish",&finishSignal,&finishRep)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
