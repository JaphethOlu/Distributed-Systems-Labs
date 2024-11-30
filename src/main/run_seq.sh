go build -buildmode=plugin ../mrapps/wc.go

rm mr-out*

go run mrsequential.go wc.so pg*.txt

sort mr-out-0 > mr-correct-wc.txt