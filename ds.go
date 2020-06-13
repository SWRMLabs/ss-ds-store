package ds

import (
	"fmt"
	"strings"
	"time"

	"github.com/StreamSpace/ss-store"
	"github.com/google/uuid"
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/query"
	logger "github.com/ipfs/go-log/v2"
)

var log = logger.Logger("store/ds")

type DSConfig struct {
	DS datastore.Batching
}

type ssDSHandler struct {
	ds datastore.Batching
}

func NewDataStore(dsConf *DSConfig) (store.Store, error) {
	return &ssDSHandler{
		ds: dsConf.DS,
	}, nil
}

func createKey(i store.Item) datastore.Key {
	return datastore.NewKey(i.GetNamespace() + "/" + i.GetId())
}

func createIndexKey(i int64, prefix string) datastore.Key {
	return datastore.NewKey(prefix + "/" + fmt.Sprintf("%d", i))
}

// DeleteIndex Func Imp
func (dsh *ssDSHandler) deleteIndex(idx datastore.Key) error {
	return dsh.ds.Delete(idx)
}

// AddIndex Func Imp
func (dsh *ssDSHandler) addIndex(idx, k datastore.Key) error {
	return dsh.ds.Put(idx, k.Bytes())
}

func (dsh *ssDSHandler) Create(i store.Item) error {
	idSetter, ok := i.(store.IDSetter)
	if ok == true {
		idSetter.SetID(uuid.New().String())
	}

	key := createKey(i)
	if timeTracker, ok := i.(store.TimeTracker); ok {
		var unixTime = time.Now().Unix()
		timeTracker.SetCreated(unixTime)
		timeTracker.SetUpdated(unixTime)
		dsh.addIndex(createIndexKey(timeTracker.GetCreated(), "create"), key)
		dsh.addIndex(createIndexKey(timeTracker.GetUpdated(), "update"), key)
	}

	value, err := i.Marshal()
	if err != nil {
		return err
	}
	return dsh.ds.Put(key, value)
}

func (dsh *ssDSHandler) Read(i store.Item) error {
	key := createKey(i)
	buf, err := dsh.ds.Get(key)
	if err != nil {
		return err
	}
	return i.Unmarshal(buf)
}

func (dsh *ssDSHandler) Update(i store.Item) error {
	key := createKey(i)
	if timeTracker, ok := i.(store.TimeTracker); ok {
		var unixTime = time.Now().Unix()
		dsh.deleteIndex(createIndexKey(timeTracker.GetUpdated(), "update"))
		timeTracker.SetUpdated(unixTime)
		dsh.addIndex(createIndexKey(timeTracker.GetUpdated(), "update"), key)
	}
	value, err := i.Marshal()
	if err != nil {
		return err
	}
	return dsh.ds.Put(key, value)
}

func (dsh *ssDSHandler) Delete(i store.Item) error {
	key := createKey(i)
	if timeTracker, ok := i.(store.TimeTracker); ok {
		dsh.deleteIndex(createIndexKey(timeTracker.GetCreated(), "create"))
		dsh.deleteIndex(createIndexKey(timeTracker.GetUpdated(), "update"))
	}
	return dsh.ds.Delete(key)
}

func (dsh *ssDSHandler) List(l store.Items, o store.ListOpt) (int, error) {
	<-time.After(time.Second * 3)
	order := o.Sort
	q := query.Query{
		Prefix: l[0].GetNamespace(),
		Limit:  int(o.Limit),
		Offset: int(o.Limit * o.Page),
	}
	listCounter := 0

	switch order {
	case store.SortNatural:
		result, _ := dsh.ds.Query(q)
		for v := range result.Next() {
			if listCounter < int(o.Limit) {
				err := l[listCounter].Unmarshal(v.Value)
				if err != nil {
					continue
				}
				listCounter++
			}
		}
	case store.SortCreatedAsc:
		log.Debug("SortCreatedAsc")
		f := filterValuePrefix{
			Prefix: q.Prefix,
		}
		c := query.OrderByKey{}
		q.Prefix = "create"
		q.Filters = []query.Filter{f}
		q.Orders = []query.Order{c}
		listCounter = dsh.getSortedResults(o.Limit, q, l)
	case store.SortCreatedDesc:
		log.Debug("SortCreatedDesc")
		f := filterValuePrefix{
			Prefix: q.Prefix,
		}
		c := orderByKeyDescending{}
		q.Prefix = "create"
		q.Filters = []query.Filter{f}
		q.Orders = []query.Order{c}
		listCounter = dsh.getSortedResults(o.Limit, q, l)
	case store.SortUpdatedAsc:
		log.Debug("SortUpdatedAsc")
		f := filterValuePrefix{
			Prefix: q.Prefix,
		}
		c := query.OrderByKey{}
		q.Prefix = "update"
		q.Filters = []query.Filter{f}
		q.Orders = []query.Order{c}
		listCounter = dsh.getSortedResults(o.Limit, q, l)
	case store.SortUpdatedDesc:
		log.Debug("SortUpdatedDesc")
		f := filterValuePrefix{
			Prefix: q.Prefix,
		}
		c := orderByKeyDescending{}
		q.Prefix = "update"
		q.Filters = []query.Filter{f}
		q.Orders = []query.Order{c}
		listCounter = dsh.getSortedResults(o.Limit, q, l)
	}

	return listCounter, nil
}

type filterValuePrefix struct {
	Prefix string
}

func (f filterValuePrefix) Filter(e query.Entry) bool {
	return strings.HasPrefix(string(e.Value), "/"+f.Prefix)
}

type orderByKeyDescending struct{}

func (o orderByKeyDescending) Compare(a, b query.Entry) int {
	return strings.Compare(b.Key, a.Key)
}

func (dsh *ssDSHandler) Close() error {
	log.Debug("closing ds")
	return nil
}

func (dsh *ssDSHandler) getSortedResults(limit int64, q query.Query, l store.Items) int {
	listCounter := 0
	result, _ := dsh.ds.Query(q)
	for v := range result.Next() {
		if listCounter < int(limit) {
			key := datastore.NewKey(string(v.Value))
			buf, err := dsh.ds.Get(key)
			if err != nil {
				log.Errorf("Unable to get data ", err.Error())
				continue
			}
			err = l[listCounter].Unmarshal(buf)
			if err != nil {
				log.Errorf("Unable to Unmarshal data ", err.Error())
				continue
			}
			listCounter++
		}
	}
	return listCounter
}
