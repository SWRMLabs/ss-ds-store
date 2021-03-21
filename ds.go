package ds

import (
	"errors"
	"fmt"
	"strings"
	"time"

	store "github.com/SWRMLabs/ss-store"
	"github.com/google/uuid"
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/query"
	logger "github.com/ipfs/go-log/v2"
)

var log = logger.Logger("store/ds")

type DSConfig struct {
	DS datastore.Batching
}

func (ds *DSConfig) Handler() string {
	return "dsdb"
}

type ssDSHandler struct {
	ds datastore.Batching
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
		ds: dsConf.DS,
	}, nil
}

func createKey(i store.Item) datastore.Key {
	return datastore.NewKey(i.GetNamespace() + "/k" + "/" + i.GetId())
}

func createdIdxKey(i store.Item) datastore.Key {
	k := fmt.Sprintf("%s/%s/%d", i.GetNamespace(), "c",
		i.(store.TimeTracker).GetCreated())
	return datastore.NewKey(k)
}

func updatedIdxKey(i store.Item) datastore.Key {
	k := fmt.Sprintf("%s/%s/%d", i.GetNamespace(), "u",
		i.(store.TimeTracker).GetUpdated())
	return datastore.NewKey(k)
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
	if timeTracker, ok := i.(store.TimeTracker); ok {
		var unixTime = time.Now().Unix()
		timeTracker.SetCreated(unixTime)
		timeTracker.SetUpdated(unixTime)
		dsh.addIndex(createdIdxKey(i), key)
		dsh.addIndex(updatedIdxKey(i), key)
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
	if timeTracker, ok := i.(store.TimeTracker); ok {
		var unixTime = time.Now().Unix()
		dsh.deleteIndex(updatedIdxKey(i))
		timeTracker.SetUpdated(unixTime)
		dsh.addIndex(updatedIdxKey(i), key)
	}
	value, err := serializableItem.Marshal()
	if err != nil {
		return err
	}
	return dsh.ds.Put(key, value)
}

func (dsh *ssDSHandler) Delete(i store.Item) error {
	key := createKey(i)
	if _, ok := i.(store.TimeTracker); ok {
		dsh.deleteIndex(createdIdxKey(i))
		dsh.deleteIndex(updatedIdxKey(i))
	}
	return dsh.ds.Delete(key)
}

func (dsh *ssDSHandler) List(
	factory store.Factory,
	o store.ListOpt,
) (store.Items, error) {
	order := o.Sort
	_, ok := factory.Factory().(store.TimeTracker)
	if order != store.SortNatural && !ok {
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
		Prefix:  factory.Factory().GetNamespace() + "/k",
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
		q.Prefix = factory.Factory().GetNamespace() + "/c"
		q.Filters = append(q.Filters, f)
		q.Orders = []query.Order{c}
		list = dsh.getSortedResults(o.Limit, q, factory)
	case store.SortCreatedDesc:
		log.Debug("SortCreatedDesc")
		f := filterValuePrefix{
			Prefix: q.Prefix,
		}
		c := orderByKeyDescending{}
		q.Prefix = factory.Factory().GetNamespace() + "/c"
		q.Filters = append(q.Filters, f)
		q.Orders = []query.Order{c}
		list = dsh.getSortedResults(o.Limit, q, factory)
	case store.SortUpdatedAsc:
		log.Debug("SortUpdatedAsc")
		f := filterValuePrefix{
			Prefix: q.Prefix,
		}
		c := query.OrderByKey{}
		q.Prefix = factory.Factory().GetNamespace() + "/u"
		q.Filters = append(q.Filters, f)
		q.Orders = []query.Order{c}
		list = dsh.getSortedResults(o.Limit, q, factory)
	case store.SortUpdatedDesc:
		log.Debug("SortUpdatedDesc")
		f := filterValuePrefix{
			Prefix: q.Prefix,
		}
		c := orderByKeyDescending{}
		q.Prefix = factory.Factory().GetNamespace() + "/u"
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
