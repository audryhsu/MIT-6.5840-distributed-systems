# Run code on word count mapreduce app

all: build-wc coordinator

# Build word-count plugin
# plugin must be compiled with same flags as main app
# IDE adds gcflags, so add to plugin build: https://youtrack.jetbrains.com/issue/GO-7474/plugin.Open-produces-plugin-was-built-with-a-different-version-of-package-math-bits-in-debug
build-wc:
	go build -buildmode=plugin -gcflags "all=-N -l" ../mrapps/wc.go

run-mrworker:
	go run mrworker.go wc.so

clean:
	rm -f mr-*-*
	rm -f mr-out-*
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
