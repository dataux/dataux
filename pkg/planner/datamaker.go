package main

import (
	"math/rand"

	"github.com/lytics/grid/grid2"
)

var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func NewDataMaker(minsize, count int) *datamaker {
	dice := grid2.NewSeededRand()
	data := make([]string, 1000)
	for i := 0; i < 1000; i++ {
		data[i] = makedata(minsize, dice)
	}

	d := &datamaker{
		count:  count,
		data:   data,
		done:   make(chan bool),
		stop:   make(chan bool),
		output: make(chan string),
	}
	go d.start()

	return d
}

type datamaker struct {
	count  int
	data   []string
	done   chan bool
	stop   chan bool
	output chan string
}

func (d *datamaker) Next() <-chan string {
	return d.output
}

func (d *datamaker) Done() <-chan bool {
	return d.done
}

func (d *datamaker) Stop() {
	close(d.stop)
}

func (d *datamaker) start() {
	sent := 0
	for {
		select {
		case <-d.stop:
			for i := 0; i < len(d.data); i++ {
				d.data[i] = ""
			}
			return
		default:
			if sent >= d.count {
				close(d.done)
				return
			}
			select {
			case <-d.stop:
				return
			case d.output <- d.data[sent%1000]:
				sent++
			}
		}
	}
}

func makedata(minsize int, dice *rand.Rand) string {
	b := make([]rune, minsize+dice.Intn(minsize))
	for i := range b {
		b[i] = letters[dice.Intn(len(letters))]
	}
	return string(b)
}
