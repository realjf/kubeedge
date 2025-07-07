package cache

import (
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/kubeedge/beehive/pkg/core/model"
	metamanagerconfig "github.com/kubeedge/kubeedge/edge/pkg/metamanager/config"

	"k8s.io/klog/v2"
)

var ServiceAccountTokenCache *serviceAccountTokenCache
var once sync.Once

func InitCache() {
	once.Do(func() {
		ServiceAccountTokenCache = newServiceAccountTokenCache()
	})
}

type serviceAccountTokenCache struct {
	lock     sync.RWMutex
	metaList map[string]*ServiceAccountTokenMeta
	f        *os.File
}

func newServiceAccountTokenCache() *serviceAccountTokenCache {
	return &serviceAccountTokenCache{
		metaList: make(map[string]*ServiceAccountTokenMeta, 0),
		f:        initCache(),
	}
}

func initCache() (cacheFile *os.File) {
	if metamanagerconfig.Config.MetaManager.ServiceAccountTokenCache == nil || !metamanagerconfig.Config.MetaManager.ServiceAccountTokenCache.Enable {
		return
	}
	cachePath := metamanagerconfig.Config.MetaManager.ServiceAccountTokenCache.Path
	if cachePath == "" {
		klog.Errorf("service account token cache path is empty")
		return
	}
	_, err := os.Stat(cachePath)
	if os.IsExist(err) {
		return
	}

	dir := filepath.Dir(cachePath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		klog.Errorf("Failed to create directory %s: %v", dir, err)
		return
	}
	f, err := os.Create(cachePath)
	if err != nil {
		klog.Errorf("Failed to create file %s: %v", cachePath, err)
		return
	}
	return f
}

func (s *serviceAccountTokenCache) Remove(key string) {
	s.lock.Lock()
	defer s.lock.Unlock()

	if _, ok := s.metaList[key]; ok {
		delete(s.metaList, key)
	}
	s.save()
}

func (s *serviceAccountTokenCache) Store(meta *ServiceAccountTokenMeta) {
	s.lock.Lock()
	defer s.lock.Unlock()
	if meta.Type != model.ResourceTypeServiceAccountToken {
		return
	}
	if s.f == nil {
		return
	}

	s.metaList[meta.Key] = meta
	s.save()
}

func (s *serviceAccountTokenCache) save() {
	data := make([]*ServiceAccountTokenMeta, 0)
	for _, v := range s.metaList {
		data = append(data, &ServiceAccountTokenMeta{
			Key:   v.Key,
			Type:  v.Type,
			Value: v.Value,
		})
	}

	payload, err := json.Marshal(data)
	if err != nil {
		klog.Errorf("Failed to json.Marshal meta %#v: %v", data, err)
		return
	}
	_, err = s.f.Write(payload)
	if err != nil {
		klog.Errorf("Failed to write '%s' to file %s: %v", string(payload), s.f.Name(), err)
		return
	}
	err = s.f.Sync()
	if err != nil {
		klog.Errorf("Failed to sync file %s: %v", s.f.Name(), err)
		return
	}
}

type ServiceAccountTokenMeta struct {
	Key   string `json:"key"`
	Type  string `json:"type"`
	Value string `json:"value"`
}

func (s *serviceAccountTokenCache) Load(key string) (meta []*ServiceAccountTokenMeta) {
	s.lock.RLock()
	defer s.lock.RUnlock()
	if o, ok := s.metaList[key]; ok {
		return []*ServiceAccountTokenMeta{o}
	}
	content, err := io.ReadAll(s.f)
	if err != nil {
		klog.Errorf("Failed to read from file %s: %v", s.f.Name(), err)
		return
	}

	if len(content) == 0 {
		return
	}

	var data []*ServiceAccountTokenMeta
	err = json.Unmarshal(content, &data)
	if err != nil {
		klog.Errorf("Failed to json.Unmarshal %s: %v", string(content), err)
		return
	}

	meta = make([]*ServiceAccountTokenMeta, 0)
	for _, v := range data {
		if v.Key == key {
			meta = append(meta, &ServiceAccountTokenMeta{
				Key:   v.Key,
				Type:  v.Type,
				Value: v.Value,
			})
		}
	}

	return meta
}

func (s *serviceAccountTokenCache) Close() {
	s.lock.Lock()
	defer s.lock.Unlock()
	if s.f != nil {
		s.f.Close()
	}
}
