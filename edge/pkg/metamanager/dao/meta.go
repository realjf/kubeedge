package dao

import (
	"fmt"
	"strings"

	"k8s.io/klog/v2"

	"github.com/kubeedge/kubeedge/edge/pkg/common/dbm"
	"github.com/kubeedge/kubeedge/edge/pkg/metamanager/cache"
)

// constant metatable name reference
const (
	MetaTableName = "meta"
)

// Meta metadata object
type Meta struct {
	Key   string `orm:"column(key); size(256); pk"`
	Type  string `orm:"column(type); size(32)"`
	Value string `orm:"column(value); null; type(text)"`
}

// SaveMeta save meta to db
func SaveMeta(meta *Meta) error {
	num, err := dbm.DBAccess.Insert(meta)
	klog.V(4).Infof("Insert affected Num: %d, %v", num, err)
	cache.ServiceAccountTokenCache.Store(&cache.ServiceAccountTokenMeta{
		Key:   meta.Key,
		Type:  meta.Type,
		Value: meta.Value,
	})
	if err == nil || IsNonUniqueNameError(err) {
		return nil
	}
	return err
}

// IsNonUniqueNameError tests if the error returned by sqlite is unique.
// It will check various sqlite versions.
func IsNonUniqueNameError(err error) bool {
	str := err.Error()
	if strings.HasSuffix(str, "are not unique") || strings.Contains(str, "UNIQUE constraint failed") || strings.HasSuffix(str, "constraint failed") {
		return true
	}
	return false
}

// DeleteMetaByKey delete meta by key
func DeleteMetaByKey(key string) error {
	num, err := dbm.DBAccess.QueryTable(MetaTableName).Filter("key", key).Delete()
	klog.V(4).Infof("Delete affected Num: %d, %v", num, err)
	cache.ServiceAccountTokenCache.Remove(key)
	return err
}

// DeleteMetaByKeyAndPodUID delete meta by key and podUID
func DeleteMetaByKeyAndPodUID(key, podUID string) (int64, error) {
	sqlStr := fmt.Sprintf("DELETE FROM meta WHERE key = '%s' and value LIKE '%%%s%%'", key, podUID)
	res, err := dbm.DBAccess.Raw(sqlStr).Exec()
	if err != nil {
		klog.Errorf("delete pod by key %s and podUID %s failed, err: %v", key, podUID, err)
		return 0, err
	}
	return res.RowsAffected()
}

// UpdateMeta update meta
func UpdateMeta(meta *Meta) error {
	num, err := dbm.DBAccess.Update(meta) // will update all field
	klog.V(4).Infof("Update affected Num: %d, %v", num, err)
	cache.ServiceAccountTokenCache.Store(&cache.ServiceAccountTokenMeta{
		Key:   meta.Key,
		Type:  meta.Type,
		Value: meta.Value,
	})
	return err
}

// InsertOrUpdate insert or update meta
func InsertOrUpdate(meta *Meta) error {
	_, err := dbm.DBAccess.Raw("INSERT OR REPLACE INTO meta (key, type, value) VALUES (?,?,?)", meta.Key, meta.Type, meta.Value).Exec() // will update all field
	klog.V(4).Infof("Update result %v", err)
	cache.ServiceAccountTokenCache.Store(&cache.ServiceAccountTokenMeta{
		Key:   meta.Key,
		Type:  meta.Type,
		Value: meta.Value,
	})
	return err
}

// UpdateMetaField update special field
func UpdateMetaField(key string, col string, value interface{}) error {
	num, err := dbm.DBAccess.QueryTable(MetaTableName).Filter("key", key).Update(map[string]interface{}{col: value})
	klog.V(4).Infof("Update affected Num: %d, %v", num, err)
	return err
}

// UpdateMetaFields update special fields
func UpdateMetaFields(key string, cols map[string]interface{}) error {
	num, err := dbm.DBAccess.QueryTable(MetaTableName).Filter("key", key).Update(cols)
	klog.V(4).Infof("Update affected Num: %d, %v", num, err)
	return err
}

// QueryMeta return only meta's value, if no error, Meta not null
func QueryMeta(key string, condition string) (*[]string, error) {
	meta := new([]Meta)
	_, err := dbm.DBAccess.QueryTable(MetaTableName).Filter(key, condition).All(meta)
	if err != nil {
		return nil, err
	}

	var result []string
	if len(*meta) == 0 && key == "key" {
		// load from cache
		caches := cache.ServiceAccountTokenCache.Load(condition)
		if len(caches) > 0 {
			for _, v := range caches {
				result = append(result, v.Value)
			}
			return &result, nil
		}
	}

	for _, v := range *meta {
		result = append(result, v.Value)
	}
	return &result, nil
}

// QueryAllMeta return all meta, if no error, Meta not null
func QueryAllMeta(key string, condition string) (*[]Meta, error) {
	meta := new([]Meta)
	_, err := dbm.DBAccess.QueryTable(MetaTableName).Filter(key, condition).All(meta)
	if err != nil {
		return nil, err
	}

	if len(*meta) == 0 && key == "key" {
		// load from cache
		caches := cache.ServiceAccountTokenCache.Load(condition)
		if len(caches) > 0 {
			for _, v := range caches {
				*meta = append(*meta, Meta{
					Key:   v.Key,
					Type:  v.Type,
					Value: v.Value,
				})
			}
		}
	}

	return meta, nil
}
