package relp

import (
	"sync"
	"fmt"
	"errors"
)

type Txn int32

type Window interface {
	Add(Txn)
	Try(Txn) error
	Remove(Txn) error
	Remaining() int
	Outstanding() int
	Size() int
	HighestAcked() Txn
	HighestSent() Txn
	Close() error
}

type empty struct{}
type semaphore chan empty

type ArrayWindow struct {
	size int
	highestAcked Txn
	highestSent Txn

	mutex *sync.Mutex
	outstanding map[Txn]bool
	semaphore semaphore
}

func NewArrayWindow(size int) Window {
	if size <= 0 {
		panic("Size must be >= 1")
	}

	return &ArrayWindow{
		size: size,
		highestAcked: 0,
		highestSent: 0,
		mutex: &sync.Mutex{},
		outstanding: make(map[Txn]bool),
		semaphore: make(semaphore, size),
	}
}

func (w *ArrayWindow) Add(txn Txn) {
	// wait until there's a free slot
	w.semaphore <- empty{}

	// prepare to mutate
	w.mutex.Lock()
	defer w.mutex.Unlock()

	if len(w.outstanding) > w.size {
		panic("w.outstanding should be < size in Add")
	}

	// client code should not reuse or resend a given message ID
	if _, present := w.outstanding[txn]; present {
		panic(fmt.Sprint("txnid already sent! ", txn))
	}

	w.outstanding[txn] = true

	// TODO: handle wraparound per spec at 999,999,999
	if txn > w.highestSent {
		w.highestSent = txn
	}
}

func (w *ArrayWindow) Try(txn Txn) error {
	panic("not impl")
}

func (w *ArrayWindow) Remove(txn Txn) error {
	w.mutex.Lock()
	defer w.mutex.Unlock()

	if _, present := w.outstanding[txn]; !present {
		return errors.New(fmt.Sprint("txn not presentin window ", txn))
	}

	if txn > w.highestAcked {
		w.highestAcked = txn
	}
	delete(w.outstanding, txn)

	// make space for another outstanding message
	_ = <- w.semaphore
	return nil
}

func (w *ArrayWindow) Size() int {
	return w.size
}


func (w *ArrayWindow) Remaining() int {
	return w.size - w.Outstanding()
}


func (w *ArrayWindow) _outstanding() int {
	return len(w.outstanding)
}

func (w *ArrayWindow) Outstanding() int {
	w.mutex.Lock()
	res := w._outstanding()
	w.mutex.Unlock()
	return res
}


func (w *ArrayWindow) HighestAcked() Txn {
	return w.highestAcked
}


func (w *ArrayWindow) HighestSent() Txn {
	return w.highestSent
}

func (w *ArrayWindow) Close() error {
	close(w.semaphore)
	return nil
}
