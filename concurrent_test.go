package influence

import "testing"

func TestConcurrentEstimateTasksStayDeterministic(t *testing.T) {
	g := influenceTestGraph()
	seedsList := [][]NodeID{
		{"a"}, {"b"}, {"c"}, {"a", "b"}, {"a", "c"}, {"b", "d"},
	}
	cfg := ModelConfig{Model: IndependentCascade, Rounds: 12, Seed: 2025}

	baseline := make([]EstimateResult, len(seedsList))
	for i, seeds := range seedsList {
		r, err := Estimate(EstimateRequest{Graph: g, Seeds: seeds, Config: cfg, Batches: 5})
		if err != nil {
			t.Fatal(err)
		}
		baseline[i] = r
	}

	// Two concurrent waves with many workers must reproduce baseline
	// exactly, despite simultaneous scheduling.
	for wave := 0; wave < 3; wave++ {
		results := make([]EstimateResult, len(seedsList))
		errs := make(chan error, len(seedsList))
		done := make(chan int, len(seedsList))
		for i := range seedsList {
			go func(i int) {
				r, err := Estimate(EstimateRequest{
					Graph: g, Seeds: seedsList[i], Config: cfg,
					Batches: 5, Workers: 3,
				})
				if err != nil {
					errs <- err
					return
				}
				results[i] = r
				done <- i
			}(i)
		}
		for range seedsList {
			<-done
		}
		close(errs)
		for err := range errs {
			t.Fatal(err)
		}
		for i := range seedsList {
			if results[i] != baseline[i] {
				t.Fatalf("wave %d task %d nondeterministic:\nbase=%+v\ngot =%+v",
					wave, i, baseline[i], results[i])
			}
		}
	}
}
