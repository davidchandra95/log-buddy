package main

import (
	"fmt"
	"log"

	"github.com/davidchandra95/proglog/internal/server"
)

func main() {
	srv := server.NewHTTPServer(":8080")

	fmt.Println("server is running..")
	log.Fatal(srv.ListenAndServe())
}
