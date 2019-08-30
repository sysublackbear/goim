package logic

import (
	"context"
	"sort"
	"strings"

	"github.com/Terry-Mao/goim/internal/logic/model"
)

var (
	_emptyTops = make([]*model.Top, 0)
)

// OnlineTop get the top online.
// 求出top几
func (l *Logic) OnlineTop(c context.Context, typ string, n int) (tops []*model.Top, err error) {
	for key, cnt := range l.roomCount {
		if strings.HasPrefix(key, typ) {
			_, roomID, err := model.DecodeRoomKey(key)
			if err != nil {
				continue
			}
			top := &model.Top{
				RoomID: roomID,
				Count:  cnt,
			}
			tops = append(tops, top)
		}
	}
	sort.Slice(tops, func(i, j int) bool {
		return tops[i].Count > tops[j].Count
	})
	if len(tops) > n {
		tops = tops[:n]
	}
	// 求出房间在线人数的topN
	if len(tops) == 0 {
		tops = _emptyTops
	}
	return
}

// OnlineRoom get rooms online.
func (l *Logic) OnlineRoom(c context.Context, typ string, rooms []string) (res map[string]int32, err error) {
	res = make(map[string]int32, len(rooms))
	for _, room := range rooms {
		// 返回入参房间列表的在线人数
		res[room] = l.roomCount[model.EncodeRoomKey(typ, room)]
	}
	return
}

// OnlineTotal get all online.
func (l *Logic) OnlineTotal(c context.Context) (int64, int64) {
	// 总的IP个数，总的连接个数（一个连接对应一个人）
	return l.totalIPs, l.totalConns
}
