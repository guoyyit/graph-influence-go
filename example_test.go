package influence_test

import (
	"fmt"

	influence "github.com/guoyyit/graph-influence-go"
)

func ExampleSimulate() {
	g, err := influence.NewGraph([]influence.NodeID{"1", "2", "3"}, []influence.Edge{
		{From: "1", To: "2", Weight: 1},
		{From: "2", To: "3", Weight: 1},
	})
	if err != nil {
		panic(err)
	}
	active, err := influence.Simulate(g,
		[]influence.NodeID{"1"},
		influence.ModelConfig{Model: influence.IndependentCascade, Rounds: 1, Seed: 1},
	)
	if err != nil {
		panic(err)
	}
	fmt.Println(active)
	// Output: [1 2 3]
}

func ExampleSelect() {
	g, err := influence.NewGraph([]influence.NodeID{"a", "b", "c"}, []influence.Edge{
		{From: "a", To: "b", Weight: 0.9},
		{From: "b", To: "c", Weight: 0.9},
	})
	if err != nil {
		panic(err)
	}
	res, err := influence.Select(influence.SelectRequest{
		Graph:   g,
		Config:  influence.ModelConfig{Model: influence.IndependentCascade, Rounds: 10, Seed: 3},
		Batches: 2,
		Budget:  1,
	})
	if err != nil {
		panic(err)
	}
	fmt.Println(res.Seeds)
	// Output: [a]
}
