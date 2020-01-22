package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/gorilla/mux"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"io"
	"net/http"
)

type User struct {
	ID           primitive.ObjectID `json:"_id,omitempty" bson:"_id,omitempty"`
	Name         string             `json:"name" bson:"name"`
	Location     string             `json:"location" bson:"location"`
	LocationType string             `json:"location_type" bson:"location_type"`
}

var (
	client     *mongo.Client
	collection *mongo.Collection
	uri        = "mongodb+srv://dbAdmin:Password@cluster0-cjr4r.gcp.mongodb.net/test?retryWrites=true&w=majority"
	ctx        = context.Background()
)

func initMongo() (client *mongo.Client) {
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri))

	if err != nil {
		fmt.Println("Connection failed!")
		panic(err)
	} else {
		err = client.Ping(ctx, readpref.Primary())
		if err != nil {
			fmt.Println("Ping error!")
			panic(err)
		} else {
			fmt.Println("Connected to MongoDB!")
		}
	}

	return client
}

func restAPI() {
	r := mux.NewRouter()
	s := r.PathPrefix("/api/v1").Subrouter()

	s.HandleFunc("/data", getPeople).Methods("GET")
	s.HandleFunc("/data/{id}", getPerson).Methods("GET")
	s.HandleFunc("/data", createPerson).Methods("POST")
	s.HandleFunc("/data/_bulk", createManyPerson).Methods("POST")
	s.HandleFunc("/data/{id}", updatePerson).Methods("PUT")
	s.HandleFunc("/data/{id}", deletePerson).Methods("DELETE")

	fmt.Printf("API started...\n\n")
	_ = http.ListenAndServe(":8000", s)
}

func getAllMongoData(person []User) map[string]interface{} {
	response := make(map[string]interface{})
	if person == nil {
		response["data"] = nil
	} else {
		response["data"] = person
	}
	response["message"] = "success"
	response["status"] = true
	return response
}

func getMongoData(id primitive.ObjectID) map[string]interface{} {
	var person User
	err := collection.FindOne(ctx, bson.M{"_id": bson.M{"$eq": id}}).Decode(&person)

	response := make(map[string]interface{})
	if err != nil {
		response["data"] = nil
		panic(err)
	} else {
		response["data"] = person
	}
	response["message"] = "success"
	response["status"] = true
	return response
}

func createMongoData(person User) map[string]interface{} {
	result, err := collection.InsertOne(ctx, person)

	response := make(map[string]interface{})
	if err != nil {
		response["data"] = nil
		response["message"] = "failed"
		response["status"] = false
	} else {
		response["data"] = result.InsertedID
		response["message"] = "success"
		response["status"] = true
	}
	return response
}

func createManyMongoData(persons []interface{}) map[string]interface{} {
	result, err := collection.InsertMany(ctx, persons)

	response := make(map[string]interface{})
	if err != nil {
		response["data"] = nil
		response["message"] = "failed"
		response["status"] = false
	} else {
		response["data"] = result.InsertedIDs
		response["message"] = "success"
		response["status"] = true
	}
	return response
}

func updateMongoData(id primitive.ObjectID, update User) map[string]interface{} {
	_, err := collection.UpdateOne(ctx, bson.M{"_id": bson.M{"$eq": id}}, bson.M{"$set": update})

	response := make(map[string]interface{})
	if err != nil {
		response["data"] = nil
		response["message"] = "failed"
		response["status"] = false
	} else {
		response["data"] = update
		response["message"] = "updated"
		response["status"] = true
	}
	return response
}

func deleteMongoData(id primitive.ObjectID) map[string]interface{} {
	_, err := collection.DeleteOne(ctx, bson.M{"_id": bson.M{"$eq": id}})

	response := make(map[string]interface{})
	if err != nil {
		response["data"] = nil
		response["message"] = "failed"
		response["status"] = false
	} else {
		response["data"] = id
		response["message"] = "deleted"
		response["status"] = true
	}
	return response
}

func getPeople(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	var people []User
	cursor, _ := collection.Find(ctx, bson.M{})
	defer cursor.Close(ctx)

	for cursor.Next(ctx) {
		var person User
		_ = cursor.Decode(&person)
		people = append(people, person)
	}

	if err := cursor.Err(); err != nil {
		panic(err)
	}

	_ = json.NewEncoder(w).Encode(getAllMongoData(people))
}

func getPerson(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	params := mux.Vars(r)
	id, _ := primitive.ObjectIDFromHex(params["id"])

	_ = json.NewEncoder(w).Encode(getMongoData(id))
}

func createPerson(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	var person User
	_ = json.NewDecoder(r.Body).Decode(&person)
	_ = json.NewEncoder(w).Encode(createMongoData(person))
}

func createManyPerson(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	var person User
	//var persons []User
	var data []interface{}

	dec := json.NewDecoder(r.Body)
	for {
		err := dec.Decode(&person)
		if err == io.EOF {
			break
		}
		if err != nil {
			panic(err)
		}

		data = append(data, person)
	}

	_ = json.NewEncoder(w).Encode(createManyMongoData(data))
}

func updatePerson(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	var (
		person User
		update User
	)

	params := mux.Vars(r)
	id, _ := primitive.ObjectIDFromHex(params["id"])

	// Get data from mongo
	err := collection.FindOne(ctx, bson.M{"_id": bson.M{"$eq": id}}).Decode(&person)
	if err != nil {
		panic(err)
	}

	// Update data with data body json
	_ = json.NewDecoder(r.Body).Decode(&update)
	update.ID = person.ID
	if update.Name == "" {
		update.Name = person.Name
	}
	if update.Location == "" {
		update.Location = person.Location
	}
	if update.LocationType == "" {
		update.LocationType = person.LocationType
	}

	_ = json.NewEncoder(w).Encode(updateMongoData(id, update))
}

func deletePerson(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	params := mux.Vars(r)
	id, _ := primitive.ObjectIDFromHex(params["id"])

	_ = json.NewEncoder(w).Encode(deleteMongoData(id))
}

func main() {
	// Start MongoDB connection
	client = initMongo()
	collection = client.Database("mongo_crud").Collection("people")

	// Start API Connection
	restAPI()
}
