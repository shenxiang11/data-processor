package main

import (
	"context"
	activitypb "data-processor/proto"
	"fmt"
	"github.com/gin-gonic/gin"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"log"
	"net/http"
	"sync"
)

func main() {

	conn, err := grpc.Dial("0.0.0.0:8001", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Did not connect: %v", err)
	}
	defer conn.Close()

	activityService := activitypb.NewActivityServiceClient(conn)

	r := gin.Default()

	r.GET("/activity/query", func(c *gin.Context) {
		var eg errgroup.Group
		var rsp sync.Map
		activitys := c.QueryArray("activitys")

		for _, id := range activitys {
			id := id
			eg.Go(func() error {
				fmt.Println(id)
				res, err := activityService.Call(context.TODO(), &activitypb.ActivityRequest{Id: id})
				if err != nil {
					rsp.Store(id, map[string]any{
						"code":    5000,
						"message": err.Error(),
						"id":      id,
					})
					return err
				}
				rsp.Store(id, res)
				return nil
			})
		}

		if err := eg.Wait(); err != nil {
			fmt.Println(err)
		}

		fmt.Println(rsp)

		json := make(map[string]any, 0)
		rsp.Range(func(key, value any) bool {
			json[key.(string)] = value
			return true
		})

		c.JSON(http.StatusOK, gin.H{
			"message": "ok",
			"data":    json,
		})

	})

	r.Run(":3001")
}
