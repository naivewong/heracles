import sys

if __name__ == "__main__":
	if len(sys.argv) != 5:
		print("usage: file column start_row end_row")
		exit(-1)

	with open(sys.argv[1], "r") as f:
		i = 0
		while i < int(sys.argv[3]):
			f.readline()
			i += 1
		while i < int(sys.argv[4]):
			print(f.readline().strip().split(",")[int(sys.argv[2])])
			i += 1