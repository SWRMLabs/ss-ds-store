package ds

import (
	"errors"
	"fmt"
	"strings"
	"time"

	store "github.com/StreamSpace/ss-store"
	"github.com/google/uuid"
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/query"
	logger "github.com/ipfs/go-log/v2"
)

var log = logger.Logger("store/ds")

type DSConfig struct {
	DS        datastore.Batching
	WithIndex bool
}

func (ds *DSConfig) Handler() string {
	return "dsdb"
}

type ssDSHandler struct {
	ds        datastore.Batching
	withIndex bool
}

type userFilter struct {
	filter  store.ItemFilter
	factory store.Factory
}

func (f userFilter) Filter(e query.Entry) bool {
	s := f.factory.Factory()
	err := s.Unmarshal(e.Value)
	if err != nil {
		return false
	}
	return f.filter.Compare(s)
}

func NewDataStore(dsConf *DSConfig) (store.Store, error) {
	return &ssDSHandler{
		ds:        dsConf.DS,
		withIndex: dsConf.WithIndex,
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
	serializableItem, ok := i.(store.Serializable)
	if ok != true {
		return errors.New("store is not Serializable")
	}
	idSetter, ok := i.(store.IDSetter)
	if ok == true {
		idSetter.SetID(uuid.New().String())
	}

	key := createKey(i)
	if timeTracker, ok := i.(store.TimeTracker); ok && dsh.withIndex {
		var unixTime = time.Now().Unix()
		timeTracker.SetCreated(unixTime)
		timeTracker.SetUpdated(unixTime)
		dsh.addIndex(createIndexKey(timeTracker.GetCreated(), i.GetNamespace()+"/create"), key)
		dsh.addIndex(createIndexKey(timeTracker.GetUpdated(), i.GetNamespace()+"/update"), key)
	}

	value, err := serializableItem.Marshal()
	if err != nil {
		return err
	}
	return dsh.ds.Put(key, value)
}

func (dsh *ssDSHandler) Read(i store.Item) error {
	serializableItem, ok := i.(store.Serializable)
	if ok != true {
		return errors.New("item is not Serializable")
	}
	key := createKey(i)
	buf, err := dsh.ds.Get(key)
	if err != nil {
		return store.ErrRecordNotFound
	}
	return serializableItem.Unmarshal(buf)
}

func (dsh *ssDSHandler) Update(i store.Item) error {
	serializableItem, ok := i.(store.Serializable)
	if ok != true {
		return errors.New("item is not Serializable")
	}

	key := createKey(i)
	if timeTracker, ok := i.(store.TimeTracker); ok && dsh.withIndex {
		var unixTime = time.Now().Unix()
		dsh.deleteIndex(createIndexKey(timeTracker.GetUpdated(), i.GetNamespace()+"/update"))
		timeTracker.SetUpdated(unixTime)
		dsh.addIndex(createIndexKey(timeTracker.GetUpdated(), i.GetNamespace()+"/update"), key)
	}
	value, err := serializableItem.Marshal()
	if err != nil {
		return err
	}
	return dsh.ds.Put(key, value)
}

func (dsh *ssDSHandler) Delete(i store.Item) error {
	key := createKey(i)
	if timeTracker, ok := i.(store.TimeTracker); ok && dsh.withIndex {
		dsh.deleteIndex(createIndexKey(timeTracker.GetCreated(), i.GetNamespace()+"/create"))
		dsh.deleteIndex(createIndexKey(timeTracker.GetUpdated(), i.GetNamespace()+"/update"))
	}
	return dsh.ds.Delete(key)
}

func (dsh *ssDSHandler) List(factory store.Factory, o store.ListOpt) (store.Items, error) {
	order := o.Sort
	if order != store.SortNatural && dsh.withIndex {
		return nil, errors.New("indexing is not supported")
	}
	queryFilters := []query.Filter{}
	if o.Filter != nil {
		filter := userFilter{
			filter:  o.Filter,
			factory: factory,
		}
		queryFilters = append(queryFilters, filter)
	}

	q := query.Query{
		Prefix:  factory.Factory().GetNamespace(),
		Limit:   int(o.Limit),
		Offset:  int(o.Limit * o.Page),
		Filters: queryFilters,
	}
	listCounter := 0
	var list []store.Item
	switch order {
	case store.SortNatural:
		result, _ := dsh.ds.Query(q)
		for v := range result.Next() {
			if listCounter < int(o.Limit) {
				serializableItem := factory.Factory()
				err := serializableItem.Unmarshal(v.Value)
				if err != nil {
					continue
				}
				list = append(list, serializableItem)
				listCounter++
			}
		}
	case store.SortCreatedAsc:
		log.Debug("SortCreatedAsc")
		f := filterValuePrefix{
			Prefix: q.Prefix,
		}
		c := query.OrderByKey{}
		q.Prefix = factory.Factory().GetNamespace() + "/create"
		q.Filters = append(q.Filters, f)
		q.Orders = []query.Order{c}
		list = dsh.getSortedResults(o.Limit, q, factory)
	case store.SortCreatedDesc:
		log.Debug("SortCreatedDesc")
		f := filterValuePrefix{
			Prefix: q.Prefix,
		}
		c := orderByKeyDescending{}
		q.Prefix = factory.Factory().GetNamespace() + "/create"
		q.Filters = append(q.Filters, f)
		q.Orders = []query.Order{c}
		list = dsh.getSortedResults(o.Limit, q, factory)
	case store.SortUpdatedAsc:
		log.Debug("SortUpdatedAsc")
		f := filterValuePrefix{
			Prefix: q.Prefix,
		}
		c := query.OrderByKey{}
		q.Prefix = factory.Factory().GetNamespace() + "/update"
		q.Filters = append(q.Filters, f)
		q.Orders = []query.Order{c}
		list = dsh.getSortedResults(o.Limit, q, factory)
	case store.SortUpdatedDesc:
		log.Debug("SortUpdatedDesc")
		f := filterValuePrefix{
			Prefix: q.Prefix,
		}
		c := orderByKeyDescending{}
		q.Prefix = factory.Factory().GetNamespace() + "/update"
		q.Filters = append(q.Filters, f)
		q.Orders = []query.Order{c}
		list = dsh.getSortedResults(o.Limit, q, factory)
	}

	return list, nil
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

func (dsh *ssDSHandler) getSortedResults(limit int64, q query.Query, f store.Factory) store.Items {
	listCounter := 0
	l := []store.Item{}
	result, _ := dsh.ds.Query(q)
	for v := range result.Next() {
		if listCounter < int(limit) {
			key := datastore.NewKey(string(v.Value))
			buf, err := dsh.ds.Get(key)
			if err != nil {
				log.Errorf("Unable to get data ", err.Error())
				continue
			}
			serializableItem := f.Factory()
			err = serializableItem.Unmarshal(buf)
			if err != nil {
				log.Errorf("Unable to Unmarshal data ", err.Error())
				continue
			}
			l = append(l, serializableItem)
			listCounter++
		}
	}
	return l
}
