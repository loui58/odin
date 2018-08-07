package connection

import (
	"strings"

	"github.com/json-iterator/go"

	elastic "gopkg.in/olivere/elastic.v5"
)

func NewElastic(cfg ElasticConfig) (client *elastic.Client, err error) {
	dsnURL := strings.Split(cfg.DSN, ",")
	client, err = elastic.NewClient(
		elastic.SetURL(dsnURL...),
		elastic.SetSniff(cfg.SetSniff),
		elastic.SetHealthcheck(cfg.SetHealthcheck),
		elastic.SetDecoder(&JsoniterDecoder{}),
	)
	return
}

type JsoniterDecoder struct{}

// Decode decodes with json.Unmarshal from the Go standard library.
func (u *JsoniterDecoder) Decode(data []byte, v interface{}) error {
	return jsoniter.ConfigFastest.Unmarshal(data, v)
}
