package main

import (
	"context"
	"github.com/IBM/sarama"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/logutil"
	"github.com/pingcap/tiflow/pkg/sink/codec/common"
	"github.com/pingcap/tiflow/pkg/sink/kafka"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"sync/atomic"
	"time"
)

var (
	data = []byte("{\"id\":0,\"database\":\"test\",\"table\":\"bc_bet_records_1000\",\"pkNames\":[\"id\"],\"isDdl\":false,\"type\":\"INSERT\",\"es\":1740570459826,\"ts\":1740570460071,\"sql\":\"\",\"sqlType\":{\"id\":-5,\"record_id\":12,\"order_no\":12,\"round_id\":12,\"platform_id\":4,\"category_id\":4,\"site_code\":12,\"site_prefix\":12,\"agent_code\":12,\"account\":12,\"third_user_name\":12,\"pull_time\":93,\"third_game_code\":12,\"all_bet\":3,\"valid_bet\":3,\"net_profit\":3,\"rake\":3,\"jackpot\":3,\"bet_time\":93,\"bet_time_stamp\":-5,\"settle_time\":93,\"settle_time_stamp\":-5,\"settle_status\":4,\"device\":12,\"bet_ip\":12,\"third_group_code\":12,\"after_balance\":3,\"is_combo\":-6,\"odds_type\":12,\"odds\":3,\"order_status\":12,\"sports_type\":12,\"winlost_time\":93,\"game_id\":4,\"currency\":12,\"settle_time_zone\":93,\"version_no\":4,\"tax_rate\":3,\"tax\":3},\"mysqlType\":{\"third_user_name\":\"varchar\",\"third_game_code\":\"varchar\",\"settle_time\":\"datetime\",\"order_status\":\"varchar\",\"record_id\":\"varchar\",\"platform_id\":\"int\",\"rake\":\"decimal\",\"winlost_time\":\"datetime\",\"id\":\"bigint\",\"order_no\":\"varchar\",\"odds_type\":\"varchar\",\"odds\":\"decimal\",\"version_no\":\"int\",\"category_id\":\"int\",\"account\":\"varchar\",\"third_group_code\":\"varchar\",\"after_balance\":\"decimal\",\"currency\":\"varchar\",\"all_bet\":\"decimal\",\"bet_time\":\"datetime\",\"bet_time_stamp\":\"bigint\",\"device\":\"varchar\",\"bet_ip\":\"varchar\",\"game_id\":\"int\",\"site_prefix\":\"varchar\",\"agent_code\":\"varchar\",\"pull_time\":\"datetime\",\"net_profit\":\"decimal\",\"is_combo\":\"tinyint\",\"tax_rate\":\"decimal\",\"round_id\":\"varchar\",\"jackpot\":\"decimal\",\"settle_time_stamp\":\"bigint\",\"settle_status\":\"int\",\"tax\":\"decimal\",\"site_code\":\"varchar\",\"valid_bet\":\"decimal\",\"sports_type\":\"varchar\",\"settle_time_zone\":\"datetime\"},\"old\":null,\"data\":[{\"id\":\"1892863501103669250\",\"record_id\":\"PG_SL_1699_1740128801023_460629\",\"order_no\":\"1892863500611616769\",\"round_id\":\"2001007_1740128800000\",\"platform_id\":\"200\",\"category_id\":\"3\",\"site_code\":\"5956\",\"site_prefix\":\"\",\"agent_code\":\"\",\"account\":\"925816148\",\"third_user_name\":\"5956agent_925816148\",\"pull_time\":\"2025-02-21 17:08:49\",\"third_game_code\":\"1543462\",\"all_bet\":\"0.500000\",\"valid_bet\":\"0.350000\",\"net_profit\":\"-0.200000\",\"rake\":\"0.000000\",\"jackpot\":\"0.000000\",\"bet_time\":\"2025-02-21 17:06:40\",\"bet_time_stamp\":\"1740128800\",\"settle_time\":\"2025-02-21 17:06:40\",\"settle_time_stamp\":\"1740128800\",\"settle_status\":\"2\",\"device\":\"\",\"bet_ip\":\"\",\"third_group_code\":\"1892863500611616769\",\"after_balance\":\"2.880000\",\"is_combo\":\"0\",\"odds_type\":\"\",\"odds\":\"0.000000\",\"order_status\":\"\",\"sports_type\":\"\",\"winlost_time\":\"2025-02-21 17:06:40\",\"game_id\":\"2001007\",\"currency\":\"BRL\",\"settle_time_zone\":\"2025-02-21 06:06:40\",\"version_no\":\"0\",\"tax_rate\":\"0.000000\",\"tax\":\"0.000000\"}]}")

	totalSend  uint64
	totalAcked uint64
)

func main() {
	err := logutil.InitLogger(&logutil.Config{
		File:                 "test.log",
		Level:                "debug",
		FileMaxSize:          300,
		FileMaxDays:          0,
		FileMaxBackups:       0,
		ZapInternalErrOutput: "stderr",
	})
	if err != nil {
		log.Panic("init log failed", zap.Error(err))
	}
	logger, err := zap.NewStdLogAt(log.L().With(zap.String("component", "sarama")), zap.DebugLevel)
	if err != nil {
		log.Error("create sarama logger failed", zap.Error(err))
	}
	sarama.Logger = logger

	option := kafka.NewOptions()
	option.MaxMessageBytes = 1024 * 1024
	option.BrokerEndpoints = []string{"127.0.0.1:9092"}
	option.ClientID = "kafka-client"
	option.Version = "3.0.0"
	option.PartitionNum = 20

	factory, err := kafka.NewSaramaFactory(option, model.DefaultChangeFeedID("sarama-test"))
	if err != nil {
		log.Error("create kafka factory failed", zap.Error(err))
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	admin, err := factory.AdminClient(ctx)
	if err != nil {
		log.Error("cannot create the admin client", zap.Error(err))
		return
	}
	defer admin.Close()

	topicName := "kafka-test"
	err = admin.CreateTopic(ctx, &kafka.TopicDetail{
		Name:              topicName,
		NumPartitions:     option.PartitionNum,
		ReplicationFactor: option.ReplicationFactor,
	}, false)
	if err != nil {
		log.Error("create topic failed", zap.Error(err))
		return
	}

	asyncProducer, err := factory.AsyncProducer(ctx, make(chan error, 1))
	if err != nil {
		log.Error("create kafka async producer failed", zap.Error(err))
		return
	}
	defer asyncProducer.Close()

	g, ctx := errgroup.WithContext(ctx)

	messageChan := make(chan *common.Message, 4096)
	g.Go(func() error {
		ticker := time.NewTicker(time.Millisecond * 1)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-ticker.C:
			}
			m := &common.Message{
				Protocol: config.ProtocolCanalJSON,
				Value:    data,
				Callback: func() {
					atomic.AddUint64(&totalAcked, 1)
				},
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case messageChan <- m:
			}
		}
	})
	g.Go(func() error {
		var (
			m         *common.Message
			partition int32
		)
		for {
			select {
			case <-ctx.Done():
				return errors.Trace(ctx.Err())
			case m = <-messageChan:
			}
			err = asyncProducer.AsyncSend(ctx, topicName, partition, m)
			if err != nil {
				log.Error("send kafka message failed", zap.Error(err))
				return errors.Trace(err)
			}
			atomic.AddUint64(&totalSend, 1)
			partition = partition % option.PartitionNum
		}
	})

	g.Go(func() error {
		statistics(ctx)
		return nil
	})

	err = asyncProducer.AsyncRunCallback(ctx)
	log.Info("program exited", zap.Error(err))
}

func statistics(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	var (
		previous      uint64
		previousAcked uint64
	)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			nowSend := atomic.LoadUint64(&totalSend)
			sendQPS := (float64(nowSend) - float64(previous)) / 5.0
			previous = nowSend

			nowAcked := atomic.LoadUint64(&totalAcked)
			ackQPS := (float64(nowAcked) - float64(previousAcked)) / 5.0
			previousAcked = nowAcked

			log.Info("kafka statistics",
				zap.Float64("send_qps", sendQPS), zap.Float64("acked_qps", ackQPS),
				zap.Uint64("total_send", nowSend), zap.Uint64("total_acked", nowAcked))
		}
	}
}
