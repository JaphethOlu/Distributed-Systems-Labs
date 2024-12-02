package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"math/rand"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	// When a map task is acquired, the intemediate files should be divided into n intermediate files fot the nReduce task.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

	lastTaskTime := time.Now()

	for {

		task, err := CallTaskRequest()

		if err != nil {
			log.Println("the rpc server could not be reached")
		}

		if task.Type == Exit {
			// Handle empty task
			elapsed := time.Since(lastTaskTime)
			if elapsed > time.Second {
				os.Exit(0)
			}
		} else if task.Type == Map {
			// Handle map task

			// Read file
			file, err := os.Open(task.Filename)
			if err != nil {
				log.Fatalf("cannot open %v", task.Filename)
			}
			content, err := io.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", task.Filename)
			}
			file.Close()
			kva := mapf(task.Filename, string(content))
			partitionedkva := Partition(kva, task.NReduce)
			interfiles := []InterFile{}
			for i, partition := range partitionedkva {
				interfile := WriteIntermediateFile(partition, task.TaskNumber, i)
				interfiles = append(interfiles, InterFile{Filename: interfile, ReducerNumber: uint(i)})
			}
			CallMapResults(task.Filename, interfiles)
			lastTaskTime = time.Now()
		} else {
			// Handle reduce task
			intermediate := []KeyValue{}
			for _, filename := range task.Files {
				kv := ReadIntermediate(filename)
				intermediate = append(intermediate, kv...)
			}
			sort.Sort(ByKey(intermediate))

			var sBuilder strings.Builder

			i := 0
			for i < len(intermediate) {
				j := i + 1
				for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, intermediate[k].Value)
				}
				output := reducef(intermediate[i].Key, values)

				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(&sBuilder, "%v %v\n", intermediate[i].Key, output)

				i = j
			}

			WriteFinalFile(sBuilder.String(), task.TaskNumber)
			CallReduceResults(task.TaskNumber)
			lastTaskTime = time.Now()
		}
	}
}

func Partition(data []KeyValue, nReduce int) [][]KeyValue {
	partitions := make([][]KeyValue, nReduce)
	for _, kv := range data {
		rNumber := ihash(kv.Key) % nReduce
		partitions[rNumber] = append(partitions[rNumber], kv)
	}
	return partitions
}

func WriteIntermediateFile(data []KeyValue, taskNumber uint, rNumber int) string {
	filename := fmt.Sprintf("mr-%d-%d.json", taskNumber, rNumber)
	filenumber := GetTempNumber()
	tempname := "temp-" + strconv.FormatUint(uint64(filenumber), 10) + "-" + filename
	file, err := os.CreateTemp(".", tempname)

	if err != nil {
		fmt.Printf("error creating file for nMap: %s, nReduce: %s\n", taskNumber, rNumber)
	}

	encoder := json.NewEncoder(file)
	if err := encoder.Encode(data); err != nil {
		fmt.Printf("error encoding JSON for nMap: %s, nReduce: %s\n", taskNumber, rNumber)
	}
	file.Close()
	if err := os.Rename(file.Name(), filename); err != nil {
		fmt.Printf("error renaming final file for nMap: %s, nReduce: %s, errror: %v\n", taskNumber, rNumber, err)
	}
	return filename
}

func ReadIntermediate(filename string) []KeyValue {
	file, err := os.Open(filename)
	defer file.Close()

	if err != nil {
		fmt.Printf("error reading file: %s for reduce task error: %v\n", filename, err)
		return nil
	}

	decoder := json.NewDecoder(file)
	kva := []KeyValue{}
	err = decoder.Decode(&kva)
	if err != nil {
		log.Fatalf("error decoding json file: %s for reduce task, error: %v", filename, err)
	}

	return kva
}

func WriteFinalFile(result string, rNumber uint) {
	filename := fmt.Sprintf("mr-out-%d", rNumber)
	filenumber := GetTempNumber()
	tempname := "temp-" + strconv.FormatUint(uint64(filenumber), 10) + "-" + filename
	file, err := os.CreateTemp(".", tempname)

	if err != nil {
		log.Fatalf("an error occured in creating a new file for reduce task: %d\n", rNumber)
	}

	file.WriteString(result)
	file.Close()
	if err := os.Rename(file.Name(), filename); err != nil {
		fmt.Printf("error renaming final file for nReduce: %s, errror: %v\n", rNumber, err)
	}
}

func MapsEqual(map1, map2 []KeyValue) {
	// Step 1: Check if lengths are different
	if len(map1) != len(map2) {
		fmt.Println("Slices are not equal in length")
	}
	// Step 2: Check if all key-value pairs are equal
	for i := 0; i < len(map1); i++ {
		if map1[i].Key != map2[i].Key && map1[i].Value != map2[i].Value {
			fmt.Println("Slices keys or values are not equal")
		}
	}
	fmt.Println("Slices are equal")
}

func GetTempNumber() uint32 {
	min := 100000000
	max := 999999999
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	return uint32(r.Intn(max-min+1) + min)
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

func CallTaskRequest() (TaskReply, error) {

	args := TaskArgs{}
	reply := TaskReply{}

	ok := call("Coordinator.TaskRequest", &args, &reply)
	if ok {
		return reply, nil
	} else {
		fmt.Printf("task request call failed!\n")
		return reply, fmt.Errorf("failed to retrieve task from the coordinator")
	}

}

func CallMapResults(filename string, interfile []InterFile) (MapResultReply, error) {

	args := MapResult{MapFilename: filename, Files: interfile}
	reply := MapResultReply{}

	ok := call("Coordinator.MapResult", &args, &reply)
	if ok {
		return reply, nil
	} else {
		fmt.Printf("map results call failed!\n")
		return reply, fmt.Errorf("failed to deliver the map result for file: %s\n", filename)
	}

}

func CallReduceResults(nReduce uint) (ReduceResultReply, error) {

	args := ReduceResult{ReducerNumber: nReduce}
	reply := ReduceResultReply{}

	ok := call("Coordinator.ReduceResult", &args, &reply)
	if ok {
		return reply, nil
	} else {
		fmt.Printf("reduce results call failed!\n")
		return reply, fmt.Errorf("failed to deliver the final results of reduce task: %d\n", nReduce)
	}

}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
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
