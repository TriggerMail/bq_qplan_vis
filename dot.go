package main

import (
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"

	"github.com/goccy/go-graphviz"
	"github.com/goccy/go-graphviz/cgraph"
)

// WriteGraph converts a job row into a graphviz graph and then writes it to the
// provided writer
func WriteGraph(w io.Writer, row *JobRow, format graphviz.Format) error {
	gv := graphviz.New()
	graph, err := gv.Graph(graphviz.Name(row.ProjectID+":"+row.JobID), graphviz.Directed)
	if err != nil {
		return err
	}

	if err := JobRowToGraph(row, graph); err != nil {
		return err
	}

	if err := gv.Render(graph, format, w); err != nil {
		return err
	}
	return gv.Close()
}

// JobRowToGraph writes a job row to a graph instance.
func JobRowToGraph(row *JobRow, graph *cgraph.Graph) error {
	// these fields will be appended to the bottom of each block
	keys := []string{
		"compute_ms_avg",
		"compute_ms_max",
		"compute_ratio_avg",
		"compute_ratio_max",
		"parallel_inputs",
		"read_ms_avg",
		"read_ms_max",
		"read_ratio_avg",
		"read_ratio_max",
		"wait_ms_avg",
		"wait_ms_max",
		"wait_ratio_avg",
		"wait_ratio_max",
		"write_ms_avg",
		"write_ms_max",
		"write_ratio_avg",
		"write_ratio_max",
		"records_read",
		"records_written",
		"completed_parallel_inputs",
		"shuffle_output_bytes_spilled",
		"shuffle_output_bytes",
	}

	maxRecordsWritten := 0
	maxComputeMsMax := 0
	for _, s := range row.JobStages {
		if s.RecordsWritten > maxRecordsWritten {
			maxRecordsWritten = s.RecordsWritten
		}
		if s.ComputeMsMax > maxComputeMsMax {
			maxComputeMsMax = s.ComputeMsMax
		}
	}

	stageMap := map[int]*JobStage{}
	nodeMap := map[int]*cgraph.Node{}
	sb := &strings.Builder{}

	for _, s := range row.JobStages {
		sb.Reset()
		n, err := graph.CreateNode(strconv.Itoa(s.ID))
		if err != nil {
			return err
		}
		score := math.Pow(float64(s.ComputeMsMax)/float64(maxComputeMsMax), 0.25)

		stageMap[s.ID] = &s
		nodeMap[s.ID] = n
		n.SetShape(cgraph.Shape("record"))
		n.SetStyle(cgraph.FilledNodeStyle)
		n.SetFillColor(fmt.Sprintf(`#%02x%02x%02x`,
			127+int(128*(1-score)),
			255,
			127+int(128*(1-score)),
		))

		sb.WriteString("{")
		fmt.Fprintf(sb, "stage %d -- %s|", s.ID, s.Name)
		fmt.Fprintf(sb, "slot_ms: %d ", s.SlotMs)
		fmt.Fprintf(sb, "records_read: %d ", s.RecordsRead)
		fmt.Fprintf(sb, "records_written: %d ", s.RecordsWritten)
		sb.WriteString(`|{steps|{`)
		for i, step := range s.Steps {
			if i > 0 {
				sb.WriteString("|")
			}
			fmt.Fprintf(sb, "{kind: %s|{", step.Kind)

			for j, sub := range step.Substeps {
				if j > 0 {
					sb.WriteString("|")
				}
				sb.WriteString(sub)
			}
			sb.WriteString("}}")
		}
		sb.WriteString(`}}|{{`)
		for i, k := range keys {
			if i > 0 {
				sb.WriteString("|")
			}
			sb.WriteString(k)
		}

		sb.WriteString("}|{")

		jsm := JobStageFieldLookup(&s)
		_ = jsm
		for i, k := range keys {
			if i > 0 {
				sb.WriteString("|")
			}
			sb.WriteString(jsm(k))
		}
		sb.WriteString(`}}}`)
		n.SetLabel(sb.String())
	}

	for _, s := range row.JobStages {
		for _, ipst := range s.InputStages {
			e, err := graph.CreateEdge("", nodeMap[ipst], nodeMap[s.ID])
			if err != nil {
				return err
			}

			penwidth := 1.0
			if maxRecordsWritten > 0 {
				penwidth = float64(stageMap[ipst].RecordsWritten) / float64(maxRecordsWritten)
				penwidth = math.Pow(penwidth, 0.25) * 20
				if penwidth < 1 {
					penwidth = 1
				}
			}
			e.SetPenWidth(penwidth)
		}
	}
	return nil
}
