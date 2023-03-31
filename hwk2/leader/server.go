package main

import (
	"bufio"
	"fmt"
	"math"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
)

/*
SOURCES CREDITS:

Tutorial from class: https://www.linode.com/docs/guides/developing-udp-and-tcp-clients-and-servers-in-go/
	** Code is borrowed from tutorial, but modified to capitalize the user input string and less print statements
Uppercase string: https://www.tutorialkart.com/golang-tutorial/golang-convert-string-to-uppercase-toupper/
Split up file into chunks: https://socketloop.com/tutorials/golang-how-to-split-or-chunking-a-file-to-smaller-pieces, https://zetcode.com/golang/readfile/

*/

// Struct for mutual exclusion to protect the map
type SafeStruct struct {
	mu           sync.Mutex
	chunks       []string       // array of the 10 chunks of input text to be processed
	chunksParsed []bool         // array that indicates whether or not each chunk has been processed
	wordmap      map[string]int // map of <word, count>
}

// Create global SafeStruct instance and set up its fields
var s = SafeStruct{chunks: make([]string, 10), chunksParsed: make([]bool, 10), wordmap: make(map[string]int)}

/*
Helper function: handleConnection() manages a new thread for each client that
connects to the server.
The function gives a chunk to be processed to the client,
waits for the client to parse the chunk, and receives the client's wordcount intermediate results.
*/
func handleConnection(c net.Conn) {
	// fmt.Println("Handling connection to client...")

	// Only exit this loop once all chunks are processed or if there is an error
	for {
		// fmt.Println("Server checking if client is ready...")

		netData, err := bufio.NewReader(c).ReadString('\n')

		// fmt.Println("Server read from channel...")

		if err != nil {
			fmt.Println(err)
			return
		}

		clientMsg := string(netData)

		// Error checking
		if strings.Compare(clientMsg, "ready\n") != 0 {
			panic("Unexpected message from client: " + clientMsg + "\n")
		}

		// fmt.Println("Server received `ready` message from client.")

		// Otherwise, carry on

		s.mu.Lock()

		// Check if there is any chunk that needs to be processed
		chunkNum := -1
		for i := range s.chunksParsed {
			if !s.chunksParsed[i] {
				chunkNum = i
				s.chunksParsed[i] = true // Mark the chunk bool as true before we exit the loop
				break
			}
		}

		s.mu.Unlock()

		// If we broke out of the loop, it's either b/c there is a chunk ready to be processed or b/c all chunks are already processed

		// If all chunks are processed, break out of loop, write output file, and close connection to worker
		if chunkNum == -1 {
			// fmt.Println("All chunks processed, sending `done` to worker...")

			// Send "done" to worker
			c.Write([]byte("done\n"))

			// Break out of loop
			break
		}

		// Otherwise, there must be a chunk to process, so carry on

		// fmt.Println("Chunk marked as true")

		// Confirm with worker that it's ready to process the chunk
		c.Write([]byte("map\n"))

		netData, err = bufio.NewReader(c).ReadString('\n')
		if err != nil {
			fmt.Println(err)
			return
		}

		clientConfirm := string(netData)

		// Error checking
		if strings.Compare(clientConfirm, "ok map\n") != 0 {
			panic("Unexpected message from client: " + clientConfirm + "\n")
		}

		// fmt.Println("Server received `ok map` message from client.")

		// Proceed with sending over the chunk to the client

		// Modify string so that all newline chars are replaced with space
		// (that way it's easy for the client to read it from the channel using \n delimiter)
		chunk := strings.ReplaceAll(s.chunks[chunkNum], "\n", " ")
		c.Write([]byte(chunk + "\n"))

		// Wait for client to parse chunk, receive results from client, add results to hashmap
		netData, err = bufio.NewReader(c).ReadString('\n')
		if err != nil {
			fmt.Println(err)
			return
		}

		clientResult := string(netData)
		clientResult = strings.TrimSuffix(clientResult, "\n") // trim off the trailing newline
		clientResultSlice := strings.Fields(clientResult)     // split the result string by space --> e.g. [achieve, 1, greatness, 3] etc.

		// Confirm to client that server received intermediate results
		c.Write([]byte("received results\n"))

		// fmt.Println("Server received intermediate results from client.")

		// Add the client results to the final hashmap

		s.mu.Lock() // Lock so that only one thread can access the map at a time

		for i := 0; i < len(clientResultSlice)-1; i += 2 {
			word := clientResultSlice[i]
			count, _ := strconv.Atoi(clientResultSlice[i+1])

			// This next part in the loop is the same as wordcount() in client
			// Make sure the word isn't an empty string - if it is then ignore it
			if word == "" {
				continue
			}

			_, ok := s.wordmap[word]

			// If the current word is already in the map, simply update its count
			if ok {
				s.wordmap[word] += count
			} else {
				// Otherwise, create a new key/value pair in the map
				s.wordmap[word] = count
			}

		}

		s.mu.Unlock() // Unlock

	}

	// At this point, broken out of the loop
	// fmt.Println("All chunks processed, creating output file...")

	// Create output directory, create output file and place it in there
	of, err := os.Create("output/output.txt")
	if err != nil {
		panic("Error in creating output file.")
	}

	defer of.Close()

	// Write output into file

	s.mu.Lock()

	for word, count := range s.wordmap {
		// fmt.Println(word + " " + strconv.Itoa(count))
		_, err2 := of.WriteString(word + " " + strconv.Itoa(count) + "\n")

		if err2 != nil {
			panic("Error in writing to output file.")
		}
	}

	s.mu.Unlock()

	// fmt.Println("Done with total word count, created output file!")

	c.Close()
}

/*
The main function of the server fetches the input file based on the input directory given in the command line.
It splits up the input file into 10 approximately equally-sized chunks (strings).
It stores the chunks in an array, along with storing booleans in an array that indicate whether or not each
chunk has been parsed by a client.
The server creates a new thread for every client that requests to connect to it, and passes any further
responsibilities to the helper function, `handleConnection`.
*/
func main() {
	arguments := os.Args
	if len(arguments) < 3 {
		fmt.Println("Please provide a port number and input directory!")
		return
	}

	PORT := ":" + arguments[1]
	l, err := net.Listen("tcp4", PORT)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer l.Close()

	// Get input directory
	directory := arguments[2]
	inputDir, err := os.ReadDir(directory)
	if err != nil {
		panic("Error when opening input directory")
	}

	// Get the single input file in the directory
	inputFilePath := directory + "/" + inputDir[0].Name()

	// Open the input file
	inputFile, err := os.Open(inputFilePath)
	if err != nil {
		panic("Could not open file " + inputFilePath)
	}

	defer inputFile.Close()

	/*
		Splitting up input:
		Create an array of strings of size 10, split up the input file into 10 chunks (strings),
		and put each chunk in the array.
	*/

	// Get file size, (file size) / 10 = size of each chunk
	fileInfo, _ := inputFile.Stat()
	var fileSize int64 = fileInfo.Size()
	var chunkSize = int64(math.Ceil(float64(fileSize / 10)))

	// Read the file - for each iteration, append a chunk of size (chunkSize) to the `chunks` array
	for i := 0; i < 10; i++ {
		buf := make([]byte, chunkSize)
		inputFile.Read(buf)

		chunk := string(buf)
		s.chunks[i] = chunk
	}

	/*
		FOR TESTING PURPOSES: to validate that the splitting of input file worked, we iterate through the array
		and for each chunk (string element of the array) that we iterate over, place that chunk
		into a small text file. This way we can open the text files and verify that the array
		was constructed correctly.
	*/
	// count := 1
	// for _, chunk := range s.chunks {
	// 	smallFileName := "testingFile" + strconv.Itoa(count)
	// 	_, err := os.Create(smallFileName)
	// 	if err != nil {
	// 		panic("Error creating smaller file " + smallFileName)
	// 	}
	// 	os.WriteFile(smallFileName, []byte(chunk), os.ModeAppend)
	// 	count++
	// }

	// Now at this point, the input file has been split into chunks
	// fmt.Println("Server split up input file.")

	// Iterate thru chunksParsed where chunksParsed[i] indicates if chunks[i] has been taken care of yet
	for i := range s.chunksParsed {
		s.chunksParsed[i] = false // Set all values to false as default
	}

	// For each client connection, create a new thread for it
	for {
		c, err := l.Accept()
		if err != nil {
			fmt.Println(err)
			return
		}
		go handleConnection(c)
	}
}
