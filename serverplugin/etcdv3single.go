package serverplugin

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/rpcxio/libkv"
	"github.com/rpcxio/libkv/store"
	estore "github.com/rpcxio/rpcx-etcd/store"
	"github.com/rpcxio/rpcx-etcd/store/etcdv3singe"
	"github.com/smallnest/rpcx/log"
	"github.com/smallnest/rpcx/share"
)

func init() {
	libkv.AddStore(estore.ETCDV3, etcdv3singe.New)
}

type RegItem struct {
	key     string
	value   []byte
	options *store.WriteOptions
}

// EtcdV3RegisterPlugin implements etcd registry.
type EtcdVSingleRegisterPlugin struct {
	// service address, for example, tcp@127.0.0.1:8972, quic@127.0.0.1:1234
	ServiceAddress string
	// etcd addresses
	EtcdServers []string
	// base path for rpcx server, for example com/example/rpcx
	BasePath string
	// Registered services
	Services  []string
	metasLock sync.RWMutex
	metas     map[string]string
	TTL       time.Duration

	Options *store.Config
	kv      store.Store

	done chan struct{}

	regItems      map[string]*RegItem
	ServerStarted chan struct{}
}

func NewEtcdV3SingleRegisterPlugin(EtcdServers []string, BasePath string, ServiceAddress string, ServerStarted chan struct{}, Options *store.Config) (*EtcdVSingleRegisterPlugin, error) {
	kv, err := libkv.NewStore(estore.ETCDV3_SINGLE, EtcdServers, nil)
	if err != nil {
		log.Errorf("cannot create etcd registry: %v", err)
		return nil, err
	}

	return &EtcdVSingleRegisterPlugin{
		EtcdServers:    EtcdServers,
		BasePath:       BasePath,
		ServiceAddress: ServiceAddress,
		Options:        Options,
		ServerStarted:  ServerStarted,
		TTL:            time.Minute,
		kv:             kv,
		regItems:       make(map[string]*RegItem),
		metas:          make(map[string]string),
		done:           make(chan struct{}),
	}, nil
}

func (p *EtcdVSingleRegisterPlugin) register() error {
	if p.ServerStarted != nil {
		<-p.ServerStarted
	}
	if share.Trace {
		log.Infof("etcd register start")
	}
	for path, value := range p.regItems {
		err := p.kv.Put(path, value.value, value.options)
		if err != nil && !strings.Contains(err.Error(), "Not a file") {
			log.Errorf("cannot create etcd path %s: %v", p.BasePath, err)
			return err
		}
	}
	return nil
}

// Start starts to connect etcd cluster
func (p *EtcdVSingleRegisterPlugin) Start() error {
	// create root path
	err := p.kv.Put(p.BasePath, []byte("rpcx_path"), &store.WriteOptions{IsDir: true, TTL: -1})
	if err != nil && !strings.Contains(err.Error(), "Not a file") {
		log.Errorf("cannot create etcd path %s: %v", p.BasePath, err)
		return err
	}

	go p.register()
	go func() {
		defer p.kv.Close()
		<-p.done
	}()

	return nil
}

// Stop unregister all services.
func (p *EtcdVSingleRegisterPlugin) Stop() error {
	for _, name := range p.Services {
		nodePath := fmt.Sprintf("%s/%s/%s", p.BasePath, name, p.ServiceAddress)
		exist, err := p.kv.Exists(nodePath)
		if err != nil {
			log.Errorf("cannot delete path %s: %v", nodePath, err)
			continue
		}
		if exist {
			p.kv.Delete(nodePath) // delete the registered node
			log.Infof("delete path %s", nodePath)
		}
	}

	close(p.done)
	return nil
}

// UpdateMetadata
func (p *EtcdVSingleRegisterPlugin) UpdateMetadata(metadata string) (err error) {
	for _, v := range p.regItems {
		if v.options.IsDir {
			continue
		}
		v.value = []byte(metadata)
	}

	p.register()
	return
}

// Register handles registering event.
// this service is registered at BASE/serviceName/thisIpAddress node
func (p *EtcdVSingleRegisterPlugin) Register(name string, rcvr interface{}, metadata string) (err error) {
	if strings.TrimSpace(name) == "" {
		err = errors.New("Register service `name` can't be empty")
		return
	}
	p.metasLock.Lock()
	// create service path
	nodePath := fmt.Sprintf("%s/%s", p.BasePath, name)
	p.regItems[nodePath] = &RegItem{nodePath, []byte(name), &store.WriteOptions{IsDir: true, TTL: -1}}

	// create node
	nodePath = fmt.Sprintf("%s/%s/%s", p.BasePath, name, p.ServiceAddress)
	p.regItems[nodePath] = &RegItem{nodePath, []byte(metadata), &store.WriteOptions{TTL: p.TTL}}

	services := make(map[string]struct{})
	for _, v := range p.Services {
		services[v] = struct{}{}
	}

	if _, ok := services[name]; !ok {
		p.Services = append(p.Services, name)
	}

	if p.ServerStarted == nil {
		p.register()
	}

	if p.metas == nil {
		p.metas = make(map[string]string)
	}
	p.metas[name] = metadata
	p.metasLock.Unlock()
	return
}

func (p *EtcdVSingleRegisterPlugin) RegisterFunction(serviceName, fname string, fn interface{}, metadata string) error {
	return p.Register(serviceName, fn, metadata)
}

func (p *EtcdVSingleRegisterPlugin) Unregister(name string) (err error) {
	if len(p.Services) == 0 {
		return nil
	}

	if strings.TrimSpace(name) == "" {
		err = errors.New("Register service `name` can't be empty")
		return
	}

	nodePath := fmt.Sprintf("%s/%s/%s", p.BasePath, name, p.ServiceAddress)
	err = p.kv.Delete(nodePath) // delete the registered node
	if err != nil {
		log.Errorf("cannot create consul path %s: %v", nodePath, err)
		return err
	}

	if len(p.Services) > 0 {
		var services = make([]string, 0, len(p.Services)-1)
		for _, s := range p.Services {
			if s != name {
				services = append(services, s)
			}
		}
		p.Services = services
	}

	p.metasLock.Lock()
	if p.metas == nil {
		p.metas = make(map[string]string)
	}
	delete(p.metas, name)
	p.metasLock.Unlock()
	return
}
