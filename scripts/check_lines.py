import sys

if __name__ == "__main__":
	if len(sys.argv) != 2:
		print("Invalid parameters number!")
		exit(-1)
	with open(sys.argv[1], "r") as f:
		line = f.readline()
		count = len(line.split(","))
		lc = 0
		while line:
			if len(line.split(",")) != count:
				print("err, count=", count, ", current num=", len(line.split(",")))
			line = f.readline()
			lc += 1
		print("lc=", lc, "count=", count)