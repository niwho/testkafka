package elasticsearch

import (
	"context"
	"encoding/json"
	"fmt"

	es "gopkg.in/olivere/elastic.v5"
)

type EsClientV5 struct {
	*es.Client
	bulk *es.BulkService
}

func CreateEsClientV5(hosts []string) (EsClientV5, error) {
	var esClient EsClientV5
	// Create a client
	// snif的作用是根据部分url获取整个集群的urls
	client, err := es.NewClient(es.SetURL(hosts[:]...), es.SetSniff(true), es.SetHealthcheck(true))
	if err == nil {
		esClient.Client = client
		esClient.bulk = client.Bulk()
	}

	return esClient, err
}

// https://github.com/olivere/elastic/issues/457
func buildEsIndexSettingsV5(shardNum int32, replicaNum int32, refreshInterval int32) string {
	return fmt.Sprintf(`{
		"settings" : {
			"number_of_shards": %d,
			"number_of_replicas": %d,
			"refresh_interval": "%ds"
		}
	}`, shardNum, replicaNum, refreshInterval)
}

func (ec EsClientV5) CreateEsIndex(index string, shardNum int32, replicaNum int32, refreshInterval int32) error {
	ctx := context.Background()
	exists, err := ec.IndexExists(index).Do(ctx)
	if err != nil {
		return err
	}
	if exists {
		return nil
	}

	body := buildEsIndexSettingsV5(shardNum, replicaNum, refreshInterval)
	_, err = ec.CreateIndex(index).BodyString(body).Do(ctx)
	if err != nil {
		return err
	}

	return nil
}

func (ec EsClientV5) DeleteEsIndex(index string) error {
	_, err := ec.DeleteIndex(index).Do(context.Background())
	if err != nil {
		return err
	}

	return nil
}

// InsertWithDocId 插入@msg
// !!! 如果@msg的类型是string 或者 []byte，则被当做Json String类型直接存进去
func (ec EsClientV5) Insert(index string, typ string, msg interface{}) error {

	// https://github.com/olivere/elastic/issues/127
	// _, err = ec.Index().Index(index).Type(typ).Id(1).BodyJson(msg).Do()
	ctx := context.Background()
	switch msg.(type) {
	case string:
		_, err := ec.Index().Index(index).Type(typ).BodyString(msg.(string)).Do(ctx)
		if err != nil {
			return err
		}

	default:
		if msgBytes, ok := msg.([]byte); ok {
			_, err := ec.Index().Index(index).Type(typ).BodyString(string(msgBytes)).Do(ctx)
			if err != nil {
				return err
			}
		} else {
			_, err := ec.Index().Index(index).Type(typ).BodyJson(msg).Do(ctx)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (ec EsClientV5) InsertWithDocId(index string, typ string, docID string, msg interface{}) error {

	ctx := context.Background()
	switch msg.(type) {
	case string:
		_, err := ec.Index().Index(index).Type(typ).Id(docID).BodyString(msg.(string)).Do(ctx)
		if err != nil {
			return err
		}

	default:
		if msgBytes, ok := msg.([]byte); ok {
			_, err := ec.Index().Index(index).Type(typ).Id(docID).BodyString(string(msgBytes)).Do(ctx)
			if err != nil {
				return err
			}
		} else {
			_, err := ec.Index().Index(index).Type(typ).Id(docID).BodyJson(msg).Do(ctx)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (ec EsClientV5) BulkInsert(index string, typ string, arr []interface{}) error {
	bulk := ec.bulk.Index(index).Type(typ)
	for _, e := range arr {
		switch e.(type) {
		case string:
			data := ([]byte)(e.(string))
			bulk.Add(es.NewBulkIndexRequest().Doc((*json.RawMessage)(&data)))
		default:
			if data, ok := e.([]byte); ok {
				bulk.Add(es.NewBulkIndexRequest().Doc((*json.RawMessage)(&data)))
			} else {
				bulk.Add(es.NewBulkIndexRequest().Doc(e))
			}
		}
	}
	if bulk.NumberOfActions() <= 0 {
		return fmt.Errorf("bulk.NumberOfActions() = %d", bulk.NumberOfActions())
	}

	rsp, err := bulk.Do(context.Background())
	if err != nil {
		return err
	}
	if rsp.Errors {
		return fmt.Errorf("BulkInsert(@arr len:%d), failed number:%#v, first fail{reason:%#v, fail detail:%#v}",
			len(arr), len(rsp.Failed()), "")
	}

	return nil
}
