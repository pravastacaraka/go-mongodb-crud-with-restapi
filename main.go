package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/gorilla/mux"
	"gopkg.in/olivere/elastic.v6"
	"net/http"
)

type User struct {
	Name         string `json:"name"`
	Location     string `json:"location"`
	LocationType string `json:"location_type"`
}

const indexName string = "massive-profiling"

func restAPI() {
	r := mux.NewRouter()
	s := r.PathPrefix("/products").Subrouter()

	s.HandleFunc("/", getAllData).Methods("GET")

	fmt.Printf("API started...\n\n")
	_ = http.ListenAndServe(":8000", s)
}

func initElastic() *elastic.Client {
	client, err := elastic.NewClient(elastic.SetURL("http://192.168.114.111:9200"))
	if err != nil {
		panic(err)
	}

	return client
}

func printResult(result *elastic.SearchResult, name string) {
	rawMsg := result.Aggregations[name]
	var ar elastic.AggregationBucketKeyItems
	err := json.Unmarshal(*rawMsg, &ar)

	if err != nil {
		fmt.Printf("Unmarshal failed: %v\n", err)
		return
	}

	for _, item := range ar.Buckets {
		fmt.Printf("[+] %v: %v\n", item.Key, item.DocCount)
	}
}

func searchWithQuery(post User) map[string]interface{} {
	client := initElastic()

	genderAggr := elastic.NewTermsAggregation().Field("gender.keyword")
	ageAggr := elastic.NewTermsAggregation().Field("age_range.keyword")

	var location string
	location = "location_default." + post.LocationType

	termName := elastic.NewWildcardQuery("name", post.Name)
	termLoc := elastic.NewWildcardQuery(location, post.Location)
	termAll := elastic.NewBoolQuery().Must(termName).Must(termLoc)

	searchResult, err := client.Search().
		Aggregation("genders", genderAggr).
		Aggregation("age_ranges", ageAggr).
		Index(indexName).
		Query(termAll).
		From(0).Size(10).
		Do(context.Background())

	if err != nil {
		fmt.Printf("Search failed: %v\n", err)
	}

	// Print result in IDE
	fmt.Println("Broken down by gender:")
	printResult(searchResult, "genders")
	fmt.Println("\nBroken down by age range:")
	printResult(searchResult, "age_ranges")
	fmt.Printf("\nTotal hits: %v\n", searchResult.Hits.TotalHits)

	// Print result in JSON
	gendersData := make(map[string]interface{})
	genders, found := searchResult.Aggregations.Terms("genders")
	if found {
		for _, b := range genders.Buckets {
			str1 := fmt.Sprintf("%v", b.Key)
			str2 := fmt.Sprintf("%v", b.DocCount)
			gendersData[str1] = str2
		}
	}
	ageRangesData := make(map[string]interface{})
	age_ranges, found := searchResult.Aggregations.Terms("age_ranges")
	if found {
		for _, b := range age_ranges.Buckets {
			str1 := fmt.Sprintf("%v", b.Key)
			str2 := fmt.Sprintf("%v", b.DocCount)
			ageRangesData[str1] = str2
		}
	}

	data := make(map[string]interface{})
	data["gender"] = gendersData
	data["age_range"] = ageRangesData

	response := make(map[string]interface{})
	response["data"] = data
	response["message"] = "success"
	response["status"] = true

	return response
}

func searchNoQuery() map[string]interface{} {
	client := initElastic()

	genderAggr := elastic.NewTermsAggregation().Field("gender.keyword")
	ageAggr := elastic.NewTermsAggregation().Field("age_range.keyword")

	searchResult, err := client.Search().
		Aggregation("genders", genderAggr).
		Aggregation("age_ranges", ageAggr).
		Index(indexName).
		From(0).Size(20).
		Do(context.Background())

	if err != nil {
		fmt.Printf("Search failed: %v\n", err)
	}

	// Print result in IDE
	fmt.Println("Broken down by gender:")
	printResult(searchResult, "genders")
	fmt.Println("\nBroken down by age range:")
	printResult(searchResult, "age_ranges")
	fmt.Printf("\nTotal hits: %v\n", searchResult.Hits.TotalHits)

	// Print result in JSON
	gendersData := make(map[string]interface{})
	genders, found := searchResult.Aggregations.Terms("genders")
	if found {
		for _, b := range genders.Buckets {
			str1 := fmt.Sprintf("%v", b.Key)
			str2 := fmt.Sprintf("%v", b.DocCount)
			gendersData[str1] = str2
		}
	}
	ageRangesData := make(map[string]interface{})
	age_ranges, found := searchResult.Aggregations.Terms("age_ranges")
	if found {
		for _, b := range age_ranges.Buckets {
			str1 := fmt.Sprintf("%v", b.Key)
			str2 := fmt.Sprintf("%v", b.DocCount)
			ageRangesData[str1] = str2
		}
	}

	data := make(map[string]interface{})
	data["gender"] = gendersData
	data["age_range"] = ageRangesData

	response := make(map[string]interface{})
	response["data"] = data
	response["message"] = "success"
	response["status"] = true

	return response
}

func getAllData(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	var post User
	_ = json.NewDecoder(r.Body).Decode(&post)

	if post.Name != "" && post.Location != "" && post.LocationType != "" {
		_ = json.NewEncoder(w).Encode(searchWithQuery(post))
	} else {
		_ = json.NewEncoder(w).Encode(searchNoQuery())
	}
}

func main() {
	client := initElastic()

	// Check index is already exist or not
	exists, err := client.IndexExists(indexName).Do(context.Background())
	if err != nil {
		panic(err)
	}
	if exists {
		fmt.Println("Index 'massive-profiling' is found")
	}

	// API Connection
	restAPI()
}
