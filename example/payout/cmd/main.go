package main

import (
	"context"

	"github.com/soyart/satch"
	"github.com/soyart/satch/datasource/smongo"
	"github.com/soyart/satch/example/payout"
)

func main() {
	ctx := context.Background()
	mg, err := smongo.NewClient(ctx, smongo.MongoDBConfig{
		Hosts:    "localhost:47017",
		Admin:    "lineman-admin",
		Username: "test_user",
		Password: "test_password",
	})
	if err != nil {
		panic(err.Error())
	}

	job := payout.New()
	ds := payout.NewDS(mg)

	satch.Start(ctx, job, ds, satch.Config{})
}
