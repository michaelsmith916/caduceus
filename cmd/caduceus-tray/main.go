package main

import (
	"fmt"

	"github.com/caduceus/caduceus/pkg/caduceus"
)

func main() {
	fmt.Printf("caduceus-tray %s\n", caduceus.Version)
	fmt.Println("Phase I tray placeholder: use caduceusctl status/config while the native tray UI is developed.")
}
