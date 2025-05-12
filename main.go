package main

import (
	"fmt"
	"time"
)

func main() {
	ch := make(chan int)

	// 生产者协程：发送数据后关闭 channel
	go func() {
		for i := 0; i < 5; i++ {
			ch <- i
			time.Sleep(100 * time.Millisecond)
		}
		close(ch) // 关闭 channel 会触发 range 退出
		fmt.Println("Channel closed")
	}()

	// 消费者协程：自动检测关闭并退出
	for v := range ch { // 当 ch 关闭时自动退出循环
		fmt.Printf("Received: %d\n", v)
	}

	fmt.Println("Channel closed, loop exited")
}
