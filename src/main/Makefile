# Run code on word count mapreduce app

all: build-wc coordinator

# Build word-count plugin
build-wc:
	go build -buildmode=plugin ../mrapps/wc.go

coordinator:
	rm -f mr-*-*
	rm -f mr-out-*
	go run mrcoordinator.go pg-dorian_gray.txt pg-grimm.txt
	#go run mrcoordinator.go pg-*.txt

worker:
	go build -buildmode=plugin ../mrapps/wc.go
	go run mrworker.go wc.so

crash-worker:
	go build -buildmode=plugin ../mrapps/crash.go
	go run mrworker.go crash.so
