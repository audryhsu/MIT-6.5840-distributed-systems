# Run code on word count mapreduce app

all: build-wc coordinator

# Build word-count plugin
build-wc:
	go build -buildmode=plugin ../mrapps/wc.go

coordinator:
	go run mrcoordinator.go pg-*.txt

worker:
	rm mr-out*
	go build -buildmode=plugin ../mrapps/wc.go
	go run mrworker.go wc.so
