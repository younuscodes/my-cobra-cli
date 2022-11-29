package cmd

import (
	"fmt"
	"strconv"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/dzwvip/oracle"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

var (
	batchSize  = 500
	numWorkers = viper.Get("ORACLE_NUM_WORKERS").(int)
	start      = time.Now()
)

func init() {
	rootCmd.AddCommand(oracleInsertCmd)
}

func oracleInsert() {
	db, err := gorm.Open(oracle.Open(viper.Get("ORACLE_USER").(string)+"/"+viper.Get("ORACLE_PASSWORD").(string)+"@"+viper.Get("ORACLE_HOST").(string)+":"+viper.Get("ORACLE_PORT").(string)+"/XE"), &gorm.Config{
		SkipDefaultTransaction: true,
		PrepareStmt:            true,
		Logger:                 logger.Default.LogMode(logger.Info)})
	if err != nil {
		log.Println(err)
		return
	}

	var (
		queue = make(chan map[string]interface{})
		wg    sync.WaitGroup
		i     = 0
		//numWorkers = runtime.NumCPU()
	)
	type dummy struct{}
	db = db.Model(&dummy{})
	//Logger: logger.Default.LogMode(logger.Error)})
	if err != nil {
		// panic error or log error info
		fmt.Println("error :", err)
	}
	data := []map[string]interface{}{}
	db = db.Table(viper.Get("ORACLE_FROM_TABLE").(string))
	db = db.Find(&data)
	//db = db.Find(&meme)
	log.Println("all result len:", len(data))
	numWorkers, _ = strconv.Atoi(viper.Get("ORACLE_NUM_WORKERS").(string))
	batchSize, _ = strconv.Atoi(viper.Get("ORACLE_BATCH_SIZE").(string))
	log.Println("batchSize len:", batchSize)
	log.Println("numWorkers len:", numWorkers)
	db = db.Table(viper.Get("ORACLE_TO_TABLE").(string))
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go OrmWorker(fmt.Sprintf("worker-%d", i), queue, &wg, db)
	}
	for _, v := range data {
		queue <- v
		i++
	}

	close(queue)
	wg.Wait()
	defer func() {
		log.Println(" Total Workers :", numWorkers)
		log.Println(" Total Documents :", len(data))
		log.Println(" Total Time taken :", time.Since(start))
	}()

}

func OrmWorker(id string, lines chan map[string]interface{}, wg *sync.WaitGroup, db *gorm.DB) {
	defer wg.Done()
	i := 0
	var bulkData []map[string]interface{}
	for data := range lines {
		i++
		bulkData = append(bulkData, data)
		if i%batchSize == 0 {
			msg := make([]map[string]interface{}, len(bulkData))
			if n := copy(msg, bulkData); n != len(bulkData) {
				log.Fatalf("%d docs in batch, but only %d copied", len(bulkData), n)
			}
			db.CreateInBatches(
				msg, len(msg),
			)
			//less data also need to show in logs
			if i%100000 == 0 {
				log.Println(" Total Time taken each 1 lac record :", i, id, time.Since(start))
			}
			bulkData = nil
		}
	}
	if len(bulkData) == 0 {
		return
	}
	msg := make([]map[string]interface{}, len(bulkData))
	copy(msg, bulkData)
	db.CreateInBatches(
		msg, len(msg),
	)
	log.Println(" final Total Time taken each 1 lac record :", i, id, time.Since(start))
}

var oracleInsertCmd = &cobra.Command{
	Use:   "ormInsert",
	Short: "insert data in oracle",
	Long:  `Insert oracle table one db to another db`,
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("Oracle insert triggered")
		oracleInsert()
	},
}
