go run -mod=vendor main.go bench write --osleep 0 --gsleep 0 --metrics $1 --batch $2 --timeseries
sleep 5
go run -mod=vendor main.go bench write --osleep 0 --gsleep 0 --metrics $1 --batch $2 --timeseries
sleep 5
go run -mod=vendor main.go bench write --osleep 0 --gsleep 0 --metrics $1 --batch $2 --timeseries