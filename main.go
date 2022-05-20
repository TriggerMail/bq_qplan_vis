package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/goccy/go-graphviz"
	"github.com/spf13/cobra"
)

func main() {
	root := cobra.Command{
		Use: "bq_qplan_vis [infile]",
		Short: "" +
			"Reads job execution info from BigQuery and produces a graph of the " +
			"job stages. The job info can be saved as DOT, SVG, or JSON. JSON " +
			"can be sent back through at a later date using 'infile' or stdin. " +
			"DOT output can be rendered to SVG, JPEG, PNG, etc. using the 'dot' " +
			"command-line tool, available by installing graphviz. Internal " +
			"rendering is handled by the graphviz library, which is embedded in " +
			"this executable.",
		RunE: cmdmain,
	}
	root.Long = root.Short + "\n\n" +
		"The display shows each stage in a shade of green. The brighter the " +
		"green, the higher the `max_compute_ms_max`. The thickness of the lines " +
		"between stages is determined by the number of output rows from the " +
		"source stage. Thick lines and green stages tend to go together, but " +
		"not always. Both are meant as a hint to the reader as to where they " +
		"might look for the problem. Note that the output can be large. It is " +
		"possible that your browser will start zoomed-in on some empty portion " +
		"of the graph. Zoom out and scroll around."

	flags := root.Flags()
	flags.SortFlags = false

	flags.String("billing_project", "", "project to query from (for billing purposes). defaults to project value.")
	flags.String("project", "", "project to query")
	flags.String("region", "region-us", "BigQuery region")
	flags.String("job_id", "", "id to query")
	flags.Bool("json", false, "output data as JSON")
	flags.Bool("dot", false, "output data as DOT")
	flags.Bool("svg", false, "output data as SVG (default)")
	flags.Duration("min_age", 0, "minimum age of the job (default 0)")
	flags.Duration("max_age", 12*time.Hour, "maximum age of the job")

	if err := root.ExecuteContext(context.Background()); err != nil {
		os.Stderr.WriteString(err.Error())
		os.Stderr.WriteString("\n")
		os.Exit(1)
	}
}

func cmdmain(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()
	cfg, err := ConfigFromFlags(cmd.Flags(), args)
	if err != nil {
		return err
	}
	var row *JobRow
	if cfg.Project != "" {
		row, err = RunQuery(ctx, cfg.BillingProject, cfg.Project, cfg.Region, cfg.JobID, cfg.Start, cfg.End)
	} else {
		dec := json.NewDecoder(cfg.Source)
		row = &JobRow{}
		err = dec.Decode(row)
		cfg.Source.Close()
	}
	if err != nil {
		return err
	}

	switch cfg.OutputMode {
	case OutputModeJSON:
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(row)
	case OutputModeDOT:
		return WriteGraph(os.Stdout, row, graphviz.XDOT)
	case OutputModeSVG:
		return WriteGraph(os.Stdout, row, graphviz.SVG)
	default:
		return fmt.Errorf("Unexpected outputmode: %v", cfg.OutputMode)
	}
}
