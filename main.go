package main

import (
	"fmt"
	"stroage-go-sdk/operation"
)

func main() {
	var sdk = new(operation.SDK)
	cfg, err := sdk.Load("cfg.toml")
	if err != nil {
		return
	}
	fmt.Printf("%v", *cfg)
}
