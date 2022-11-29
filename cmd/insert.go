package cmd

import (
	"fmt"
	"strconv"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/jmoiron/sqlx"

	_ "github.com/godror/godror"
)

func init() {
	rootCmd.AddCommand(insertCmd)
}

func Insert() {
	fmt.Printf("ORACLE_PASSWORD here: %v\n", viper.Get("ORACLE_PASSWORD"))
	db, err := sqlx.Connect("godror", viper.Get("ORACLE_USER").(string)+"/"+viper.Get("ORACLE_PASSWORD").(string)+"@"+viper.Get("ORACLE_HOST").(string)+":"+viper.Get("ORACLE_PORT").(string)+"/XE")
	if err != nil {
		fmt.Println(err)
		return
	}
	defer db.Close()

	numWorkers, _ = strconv.Atoi(viper.Get("ORACLE_NUM_WORKERS").(string))
	batchSize, _ = strconv.Atoi(viper.Get("ORACLE_BATCH_SIZE").(string))

	var (
		queue = make(chan map[string]interface{})
		wg    sync.WaitGroup
		i     = 0
	)
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go Worker(fmt.Sprintf("worker-%d", i), queue, &wg, db)
	}

	personMaps := []map[string]interface{}{}
	rows, err := db.Queryx("SELECT * FROM " + viper.Get("ORACLE_FROM_TABLE").(string))
	if err != nil {
		fmt.Println(err)
	}
	defer rows.Close()
	// map
	for rows.Next() {
		results := make(map[string]interface{})
		err = rows.MapScan(results)
		if err != nil {
			fmt.Println(err)
		}
		personMaps = append(personMaps, results)
	}

	for _, v := range personMaps {
		queue <- v
		i++
	}

	close(queue)
	wg.Wait()
	defer func() {
		log.Println(" Total Workers :", numWorkers)
		log.Println(" Total Documents :", len(personMaps))
		log.Println(" Total Time taken :", time.Since(start))
	}()

}

func Worker(id string, lines chan map[string]interface{}, wg *sync.WaitGroup, db *sqlx.DB) {
	defer wg.Done()
	i := 0
	query := "INSERT INTO " + viper.Get("ORACLE_TO_TABLE").(string) + "(CATEGORYID, NAME, SKU,URI) VALUES " + "(:CATEGORYID, :NAME, :SKU, :URI)"
	var bulkData []map[string]interface{}
	for data := range lines {
		i++
		bulkData = append(bulkData, data)
		if i%batchSize == 0 {
			msg := make([]map[string]interface{}, len(bulkData))
			if n := copy(msg, bulkData); n != len(bulkData) {
				log.Fatalf("%d docs in batch, but only %d copied", len(bulkData), n)
			}

			for _, v := range msg {
				_, err := db.NamedExec(query, v)

				if err != nil {
					fmt.Println("Insert Error :", err)
				}
			}

			//less data also need to show in logs
			if i%100000 == 0 {
				log.Println(" Total Time taken each 1000 lac record :", i, id, time.Since(start))
			}
			bulkData = nil
		}
	}
	if len(bulkData) == 0 {
		return
	}
	msg := make([]map[string]interface{}, len(bulkData))
	copy(msg, bulkData)
	for _, v := range msg {
		_, err := db.NamedExec(query, v)

		if err != nil {
			fmt.Println("Insert Error :", err)
		}
	}
	log.Println(" final Total Time taken each 1 lac record :", i, id, time.Since(start))
}

var insertCmd = &cobra.Command{
	Use:   "insert",
	Short: "insert data in oracle",
	Long:  `Insert oracle table one db to another db`,
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("Oracle insert triggered")
		Insert()
	},
}
