package main

import (
	"context"
	"encoding/json"
	"log"
	"os"

	"github.com/soyart/satch/datasource/smongo"
	"github.com/soyart/satch/example/payout"
)

func main() {
	ctx := context.Background()

	mg, err := smongo.NewClient(ctx, smongo.MongoDBConfig{
		Hosts:    "localhost:47017",
		Admin:    "admin",
		Username: "test_user",
		Password: "test_password",
	})
	if err != nil {
		panic(err)
	}

	var customers []payout.Customer
	var accounts []payout.Account
	var payouts []payout.Payout

	load("./example/payout/mock/customers.json", &customers)
	load("./example/payout/mock/accounts.json", &accounts)
	load("./example/payout/mock/payouts.json", &payouts)

	db := mg.Unwrap().Database(payout.DB)
	_, err = smongo.InsertMany(ctx, db.Collection(payout.CollectionCustomers), sliceInf(customers))
	if err != nil {
		panic(err)
	}

	_, err = smongo.InsertMany(ctx, db.Collection(payout.CollectionAccounts), sliceInf(accounts))
	if err != nil {
		panic(err)
	}

	_, err = smongo.InsertMany(ctx, db.Collection(payout.CollectionPayouts), sliceInf(payouts))
	if err != nil {
		panic(err)
	}
}

func load(filename string, result interface{}) {
	b, err := os.ReadFile(filename)
	if err != nil {
		log.Printf("failed to read file '%s'", filename)
		panic(err)
	}

	err = json.Unmarshal(b, &result)
	if err != nil {
		log.Printf("failed to unmarshal from file '%s'", filename)
		panic(err)
	}
}

func sliceInf[T any](data []T) []interface{} {
	result := make([]interface{}, len(data))
	for i := range data {
		result[i] = data[i]
	}

	return result
}
