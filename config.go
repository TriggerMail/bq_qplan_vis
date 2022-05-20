package main

import (
	"errors"
	"os"
	"strings"
	"time"

	"github.com/mattn/go-isatty"

	"github.com/spf13/pflag"
)

type (
	OutputMode uint8
	InputMode  uint8
)

const (
	OutputModeSVG = OutputMode(iota)
	OutputModeDOT
	OutputModeJSON
)

type Config struct {
	BillingProject string
	Project        string
	Region         string
	JobID          string
	OutputMode     OutputMode
	InputMode      InputMode
	Source         *os.File
	Start          time.Time
	End            time.Time
}

func ConfigFromFlags(flags *pflag.FlagSet, args []string) (*Config, error) {
	cfg := Config{
		OutputMode: OutputModeSVG,
	}
	if useJson, err := flags.GetBool("json"); err != nil {
		return nil, err
	} else if useJson {
		cfg.OutputMode = OutputModeJSON
	}
	if useDot, err := flags.GetBool("dot"); err != nil {
		return nil, err
	} else if useDot {
		cfg.OutputMode = OutputModeDOT
	}

	if len(args) > 1 {
		return nil, errors.New("only one input file can be handled at a time")
	}
	if len(args) > 0 {
		f, err := os.Open(args[0])
		if err != nil {
			return nil, err
		}
		cfg.Source = f
		return &cfg, nil
	} else if !isatty.IsTerminal(os.Stdin.Fd()) {
		cfg.Source = os.Stdin
		return &cfg, nil
	}

	var err error
	cfg.Project, err = flags.GetString("project")
	if err != nil {
		return nil, err
	}

	cfg.BillingProject, err = flags.GetString("billing_project")
	if err != nil {
		return nil, err
	}
	if len(cfg.BillingProject) == 0 {
		cfg.BillingProject = cfg.Project
	}

	cfg.Region, err = flags.GetString("region")
	if err != nil {
		return nil, err
	}

	if !strings.HasPrefix(cfg.Region, "region-") {
		cfg.Region = "region-" + cfg.Region
	}

	cfg.JobID, err = flags.GetString("job_id")
	if err != nil {
		return nil, err
	}

	if cfg.BillingProject == "" {
		return nil, errors.New("parameter 'billing_project' is required if 'infile' is not specified")
	}

	if cfg.Project == "" {
		return nil, errors.New("parameter 'project' is required if 'infile' is not specified")
	}

	if cfg.JobID == "" {
		return nil, errors.New("parameter 'job_id' is required if 'infile' is not specified")
	}

	minAge, err := flags.GetDuration("min_age")
	if err != nil {
		return nil, err
	}
	if minAge > 0 {
		minAge *= -1
	}

	maxAge, err := flags.GetDuration("max_age")
	if err != nil {
		return nil, err
	}
	if maxAge > 0 {
		maxAge *= -1
	}

	cfg.End = time.Now().Add(minAge)
	cfg.Start = time.Now().Add(maxAge)

	return &cfg, nil
}
