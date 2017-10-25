package relp

import (
	"testing"
	"time"
	"math/rand"
	"log"
)

func MakeWindow(t *testing.T, size int) Window {
	t.Helper()
	return NewArrayWindow(size)
}

func TestSize(t *testing.T) {
	window := MakeWindow(t, 10)
	if window.Size() != 10 {
		t.Error("Size was not 10 as expected", window.Size())
	}
}


func TestRemainingEmpty(t *testing.T) {
	window := MakeWindow(t, 10)
	if window.Remaining() != 10 {
		t.Error("Remaining was not 10 as expected", window.Remaining())
	}
}

func TestAddOne(t *testing.T) {
	window := MakeWindow(t, 10)
	window.Add(5)
	if window.Remaining() != 9 {
		t.Error("Adding should have reduced capacity by 1")
	}
	if window.Outstanding() != 1 {
		t.Error("Adding should have set outstanding to 1")
	}
}

func TestAddAndRemoveOne(t *testing.T) {
	window := MakeWindow(t, 10)
	window.Add(5)
	if err := window.Remove(5); err != nil {
		t.Error("Remove should have succeeded", err)
	}
	if window.Remaining() != 10 {
		t.Error("Should be empty")
	}
}


func TestRemoveUnsentOne(t *testing.T) {
	window := MakeWindow(t, 10)
	if err := window.Remove(5); err == nil {
		t.Error("Removing an unsent txn ID should have failed")
	}
}

func TestFull(t *testing.T) {
	window := MakeWindow(t, 2)
	window.Add(20)
	window.Add(21)
	if window.Remaining() != 0 {
		t.Fatal("Window should be full")
	}

	go func() {
		window.Add(22)
	}()

	time.Sleep(1 * time.Second)
	if window.HighestSent() != 21 {
		t.Fatal("Send of 22 should have blocked")
	}

	window.Remove(20)
	time.Sleep(1 * time.Second)
	if window.HighestSent() != 22 {
		t.Fatal("Send of 22 should have succeeded")
	}
}

func ShuffleTxns(t *testing.T, slice []Txn) {
	t.Helper()
	for i := len(slice) - 1; i > 0; i-- {
		j := rand.Intn(i + 1)
		slice[i], slice[j] = slice[j], slice[i]
	}
}

func TestContention(t *testing.T) {
	window := MakeWindow(t, 250)

	rand.Seed(time.Now().UnixNano())

	addedChan := make(chan Txn, 200)
	removeChan := make(chan Txn, 200)

	elements := 10000

	go func() {
		for i := Txn(1); i <= Txn(elements); i++ {
			window.Add(i)
			addedChan <- i
		}
	}()

	var removeErr error = nil

	maxChunkLen := 30
	go func() {
		count := elements
		for {
			if count == 0 {
				log.Print("No more elements! to pull from addChan")
				return
			}
			chunkLen := count
			if chunkLen > maxChunkLen {
				chunkLen = maxChunkLen
			}
			slice := make([]Txn, chunkLen)
			for i := 0; i < chunkLen; i++ {
				slice[i] = <- addedChan
			}
			ShuffleTxns(t, slice)
			for i := 0; i < chunkLen; i++ {
				if err := window.Remove(slice[i]); err != nil {
					removeErr = err
				}
				removeChan <- slice[i]
			}
			count -= chunkLen
		}
	}()

	for i := 0; i < elements; i++ {
		// log.Print("Removing...", i)
		<- removeChan
	}

	if removeErr != nil {
		t.Error("Had at least one error during removal ", removeErr)
	}

	if window.Outstanding() != 0 {
		t.Error("Should have 0 outstanding")
	}

	if window.HighestAcked() != Txn(elements) {
		t.Error("HighestAcked should be ", elements)
	}

	if window.HighestSent() != Txn(elements) {
		t.Error("HighestSent should be ", elements)
	}
}
