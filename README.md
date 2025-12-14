# go-flow


##  启动示例
```shell
go run main.go
```
###  示例输出
```
2025/12/14 10:41:37 operator `root` start at 2025-12-14 10:41:37
2025/12/14 10:41:37 this is default_root
2025/12/14 10:41:37 operator `root` end  cost[51 ms]
2025/12/14 10:41:37 operator `subflow` start at 2025-12-14 10:41:37
2025/12/14 10:41:37 operator `root` start at 2025-12-14 10:41:37
2025/12/14 10:41:37 this is default_root
2025/12/14 10:41:37 operator `root` end  cost[51 ms]
2025/12/14 10:41:37 operator `end` start at 2025-12-14 10:41:37
2025/12/14 10:41:37 this is default_root
2025/12/14 10:41:37 operator `end` end  cost[51 ms]
2025/12/14 10:41:37 operator `subflow` end  cost[102 ms]
2025/12/14 10:41:37 operator `end` start at 2025-12-14 10:41:37
2025/12/14 10:41:37 this is default_root
2025/12/14 10:41:37 operator `end` end  cost[51 ms]
```
