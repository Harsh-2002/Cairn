package main

import (
	"fmt"
	"os"

	"github.com/Harsh-2002/Cairn/internal/cli"
)

func main() {
	if err := cli.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, "cairn:", err)
		os.Exit(1)
	}
}
