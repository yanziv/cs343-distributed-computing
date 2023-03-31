package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"regexp"
	"strings"
	"sync"
)

var wordRegExp = regexp.MustCompile(`\pL+('\pL+)*`)

func single_threaded(files []string) {

	//writing to a file named "single.txt" in the "output" folder
	f, err := os.Create("output/single.txt")
	if err != nil {
		log.Fatal(err)
	}

	var all_content = ""
	for _, file := range files {
		fmt.Println(file)

		readFile, err := ioutil.ReadFile("input/" + file) // data := os.ReadFile(files)//readFile, err := os.Open(file)
		if err != nil {
			fmt.Println("Error reading file:", err)
			return
		}
		defer f.Close()

		//convert readFile to string type
		//turn everything in the file to lowercase
		entire_content := strings.ToLower(string(readFile))
		all_content += entire_content
	}

	//TODO: *****PROBLEM HERE:CANT READ FROM big.txt somehow*********
	freq := make(map[string]int) //create a hashmap that counts word frequency

	//replace all punctuation from entire_content string with an empty string
	reg, err := regexp.Compile("[^a-zA-Z]+")
	if err != nil {
		log.Fatal(err)
	}

	cleaned_content := reg.ReplaceAllString(all_content, " ")

	cleaned_content2 := strings.ToLower(cleaned_content)

	//split the cleaned_content by space and append words to a list
	words := strings.Fields(cleaned_content2)

	for _, word2 := range words { //word1
		freq[word2]++
	}

	//write word:frequency pairs into our output file
	for key, value := range freq {
		final_string := fmt.Sprintf("%s %d \n", key, value)
		_, err := f.WriteString(final_string) // returns number of bytes written and err
		if err != nil {
			log.Fatal(err)
			return
		}
	}

	fmt.Println("SingleThreaded File created and content is successfully written")

}

// SafeCounter is safe to use concurrently.
type SafeCounter struct { //custom datatype
	mu sync.Mutex //mu field protects v field
	// produce mutual exclusion to protect shared resources from being accessed concurrently
	// by multiple goroutines
	v map[string]int
}

func (c *SafeCounter) Increase(key string) {
	/**
	Method defined in Safecounter struct incrementing value with given key in map
	**/
	c.mu.Lock()
	defer c.mu.Unlock()
	c.v[key]++
}

func (c *SafeCounter) Value(key string) int {
	/**
	Value() return value associated with a given key in the map
	**/
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.v[key]
}

func chunkSliceStringArr(slice []string, chunkSize int) []string { //[][]byte to return
	//var chunks [][]byte
	var chunks []string
	for i := 0; i < len(slice); i += chunkSize {
		end := i + chunkSize

		// necessary check to avoid slicing beyond
		// slice capacity
		if end > len(slice) {
			end = len(slice)
		}
		for _, s := range slice[i:end] {
			chunks = append(chunks, s)
		}
	}
	return chunks
}

var contentAll []string

func readFile(files []string) []string {
	for _, file := range files {

		readFile, err := ioutil.ReadFile("input/" + file) //return file object
		if err != nil {
			fmt.Println("Error reading File", err)
			//return
		}

		//bytes.Join(chunks, []byte(" "))
		content := strings.ToLower(string(readFile))
		reg, err := regexp.Compile("[^a-zA-Z]+")
		if err != nil {
			log.Fatal(err)
		}

		cleaned_content := reg.ReplaceAllString(content, " ")
		contentAll = append(contentAll, cleaned_content)
	}
	return contentAll
}

func multi_threaded(files []string) {
	// lock and the dictionary should be in the same struct

	f, err := os.Create("output/multi.txt")
	if err != nil {
		log.Fatal(err)
	}

	contentAll := readFile(files)
	counter := SafeCounter{v: make(map[string]int)}
	//v field initialized with empty map, allocate and return new map
	// used to count frequency of word in thread-safe manner to increment and retrieve count

	var wg sync.WaitGroup
	chunks := chunkSliceStringArr(contentAll, int(len(contentAll)/3))
	for _, chunk := range chunks {
		wg.Add(1)

		go func(s string) {
			defer wg.Done()

			counting(s, &counter)
			//pointer to SafeCounter struct used to obtain
			//memory address of the counter variable count the words and update SafeCounter struct

		}(string(chunk))
	}
	wg.Wait()

	count := make([]WordCount, 0, len(counter.v))

	for k, v := range counter.v {
		count = append(count, WordCount{k, v}) //k= string, v = int
	}

	fmt.Printf("count: %T \n", count)
	wordOutput := make(map[string]int)
	for _, wc := range count {
		wordOutput[wc.word] = wc.count
	}
	fmt.Printf("wordOutput: %T \n", wordOutput)

	for key, value := range wordOutput {
		final_string := fmt.Sprintf("%s %d \n", key, value)
		_, err := f.WriteString(final_string) // returns number of bytes written and err
		if err != nil {
			log.Fatal(err)
			return
		}
	}

	fmt.Println("Multithreading File created and content is successfully written")
}

type WordCount struct {
	word  string
	count int
}

func counting(content string, counter *SafeCounter) {

	reg, err := regexp.Compile("[^a-zA-Z]+")
	if err != nil {
		log.Fatal(err)
	}

	cleaned_content := reg.ReplaceAllString(content, " ")

	words := strings.Fields(cleaned_content)
	for _, word := range words {
		counter.Increase(strings.ToLower(word))
	}

}

func main() {
	// read directory from org
	// check if it is only one and existing argument (folder name)
	arguments := os.Args
	if len(arguments) == 1 {
		fmt.Println("Please give one argument.")
		return
	}
	path := arguments[1]
	//fmt.Println(path)

	_, err := os.Stat(path)
	if err != nil {
		fmt.Println("Path does not exist!", err)
	}

	//process directory from arg
	files, err := ioutil.ReadDir(path)
	if err != nil {
		log.Fatal(err)
	}

	var file_array []string
	for _, file := range files {
		file_array = append(file_array, file.Name())
	}
	fmt.Println("file_array!")
	fmt.Println(file_array)

	//single_threaded(file_array)
	multi_threaded(file_array)

}
