package comet

import (
	"github.com/Terry-Mao/goim/api/comet/grpc"
	"github.com/Terry-Mao/goim/internal/comet/conf"
	"github.com/Terry-Mao/goim/internal/comet/errors"
	log "github.com/golang/glog"
)

// Ring ring proto buffer.
type Ring struct {
	// read
	rp   uint64
	num  uint64
	mask uint64
	// TODO split cacheline, many cpu cache line size is 64
	// pad [40]byte
	// write
	wp   uint64
	data []grpc.Proto
}

// NewRing new a ring buffer.
func NewRing(num int) *Ring {
	r := new(Ring)  // 两段式初始化
	r.init(uint64(num))
	return r
}

// Init init ring.
func (r *Ring) Init(num int) {
	r.init(uint64(num))
}

// 求出最小的N，使得2^N >= num
func (r *Ring) init(num uint64) {
	// 2^N
	if num&(num-1) != 0 {
		for num&(num-1) != 0 {
			num &= (num - 1)
		}
		num = num << 1
	}
	r.data = make([]grpc.Proto, num)
	r.num = num
	r.mask = r.num - 1
}

// Get get a proto from ring.
func (r *Ring) Get() (proto *grpc.Proto, err error) {
	if r.rp == r.wp {
		return nil, errors.ErrRingEmpty  // 环形数据为空
	}
	proto = &r.data[r.rp&r.mask]  // 找出那一个包
	return
}

// GetAdv incr read index.
func (r *Ring) GetAdv() {
	r.rp++  // 底层rp，打印debug log，显示出现在读到哪个位置
	if conf.Conf.Debug {
		log.Infof("ring rp: %d, idx: %d", r.rp, r.rp&r.mask)
	}
}

// Set get a proto to write.
func (r *Ring) Set() (proto *grpc.Proto, err error) {
	if r.wp-r.rp >= r.num {  // 已经写满了
		return nil, errors.ErrRingFull
	}
	proto = &r.data[r.wp&r.mask]
	return
}

// SetAdv incr write index.
func (r *Ring) SetAdv() {
	r.wp++
	if conf.Conf.Debug {
		log.Infof("ring wp: %d, idx: %d", r.wp, r.wp&r.mask)
	}
}

// Reset reset ring.
func (r *Ring) Reset() {
	r.rp = 0
	r.wp = 0
	// prevent pad compiler optimization
	// r.pad = [40]byte{}
}
