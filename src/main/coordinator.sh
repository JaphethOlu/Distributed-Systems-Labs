go build -buildmode=plugin ../mrapps/wc.go

rm ./mr-*

go run mrcoordinator.go pg-*.txt