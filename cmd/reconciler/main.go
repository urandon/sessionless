package main

import (
	"log"
	"os"

	"gitcode.com/urandon/sessionless/internal/skeleton"
)

func main() {
	if err := skeleton.Run("reconciler", os.Stdout); err != nil {
		log.Fatal(err)
	}
}
