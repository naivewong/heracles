import sys

if __name__ == "__main__":
	lines = sys.argv[1]
	start = int(sys.argv[2])
	end = int(sys.argv[3])
	print("------------------------------------")
	for line in lines.split("\n"):
		line = line.split(",")
		print(",".join(line[start:end]))