package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"regexp"
	"strconv"
	"strings"
)

/*
SOURCES CREDITS:

Tutorial from class: https://www.linode.com/docs/guides/developing-udp-and-tcp-clients-and-servers-in-go/
	** Code is borrowed from tutorial, but modified to capitalize the user input string and less print statements

*/

/*
wordcount() takes in a string to be parsed and returns a string of the word counts.
Uses an intermediary hashmap to store the wordcounts before formatting it into a string.
*/
func wordcount(chunk string) string {
	wordmap := make(map[string]int)

	chunk = strings.ToLower(chunk)
	wordDelimiter := regexp.MustCompile(`( +)|(?:--)`) // delimiter that means any amount of whitespace OR '--'
	words := wordDelimiter.Split(chunk, -1)            // returns a slice of all the words after splitting

	// Regular expression string that means "all non-alphanumeric characters", use this for filtering words later on
	nonAlphaNum := regexp.MustCompile(`[^a-zA-Z0-9 ]+`)

	// For each word in the chunk
	for _, word := range words {
		// Filter the word to replace any nonalphanumeric chars w/ an empty string
		word = nonAlphaNum.ReplaceAllString(word, "")

		// Make sure the word isn't an empty string - if it is then ignore it
		if word == "" {
			continue
		}

		_, ok := wordmap[word]

		// If the current word is already in the map, simply update its count
		if ok {
			wordmap[word] += 1
		} else {
			// Otherwise, create a new key/value pair in the map
			wordmap[word] = 1
		}
	}
	// At this point we've finished counting the words in the chunk

	// Format the hashmap into a string to return
	result := ""
	for word, count := range wordmap {
		result += (word + " " + strconv.Itoa(count) + " ")
	}

	// Return result: e.g. "achieve 1 afraid 1 greatness 3 " etc.
	return result
}

/*
The main function of the client connects to the server, receives a chunk of input text to parse from the server,
runs a wordcount on the chunk, and sends that chunk's wordcount to the server as an intermediate result.
*/
func main() {
	arguments := os.Args
	if len(arguments) == 1 {
		fmt.Println("Please provide host:port.")
		return
	}

	CONNECT := arguments[1]
	c, err := net.Dial("tcp", CONNECT)
	if err != nil {
		fmt.Println(err)
		return
	}

	// fmt.Println(">> Client connected to leader...")

	for {
		// fmt.Println("Client is ready...")

		// Send "ready" to leader
		c.Write([]byte("ready\n"))

		// fmt.Println("Client sent ready message.")

		// Receive message from leader
		message, err := bufio.NewReader(c).ReadString('\n')
		if err != nil {
			fmt.Println(err)
			return
		}

		// If received "done" from leader, exit client
		if message == "done\n" {
			fmt.Println("TCP client exiting...")
			return
		}

		// Otherwise, proceed
		if message == "map\n" {
			// Send back "ok map" confirmation to leader
			c.Write([]byte("ok map\n"))

			// Receive chunk of input text from leader
			chunk, _ := bufio.NewReader(c).ReadString('\n')
			chunk = strings.TrimSuffix(chunk, "\n") // Trim off the trailing \n that was used for delimiter

			// Parse the chunk w/ wordcount helper function
			chunkResult := wordcount(chunk) + "\n"

			// Send back intermediate results to the leader
			c.Write([]byte(chunkResult))

			// Confirm w/ leader that the leader received the worker's intermediate results
			serverConfirm, err := bufio.NewReader(c).ReadString('\n')
			if err != nil {
				fmt.Println(err)
				return
			}

			if strings.Compare(serverConfirm, "received results\n") != 0 {
				panic("Did not receive server confirmation of results: received `" + serverConfirm + "`.\n")
			}
		}
	}
}
